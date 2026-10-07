"""
dags/reverse_etl_airflow_dag.py
-------------------------------
Apache Airflow DAG for Scheduled Reverse ETL & Customer Segmentation.

Workflow Steps:
  1. extract_and_validate_task
  2. transform_and_segment_task
  3. load_warehouse_task
  4. reverse_etl_crm_push_task
  5. notify_and_audit_task

Dependency Graph:
  extract_and_validate >> transform_and_segment >> load_warehouse >> reverse_etl_crm_push >> notify_and_audit
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path for module discovery in Airflow workers
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Airflow standard imports
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


def _run_extract(**context):
    from src.config_loader import load_config
    from src.extractor import extract
    
    config = load_config()
    csv_path = ROOT_DIR / config["paths"]["input_csv"]
    df = extract(csv_path)
    if df is None or df.empty:
        raise ValueError("Extraction returned empty dataset.")
    print(f"Airflow Task [Extract]: Extracted {len(df)} records from {csv_path}")
    # Pass metadata via XCom if required
    return len(df)


def _run_transform(**context):
    from src.config_loader import load_config
    from src.extractor import extract
    from src.transformer import transform
    
    config = load_config()
    csv_path = ROOT_DIR / config["paths"]["input_csv"]
    raw_df = extract(csv_path)
    transformed_df = transform(
        raw_df,
        rfm_config=config["rfm"],
        seg_config=config["segmentation"]
    )
    if transformed_df is None or transformed_df.empty:
        raise ValueError("Transformation produced empty dataframe.")
    segments = transformed_df["segment"].value_counts().to_dict()
    print(f"Airflow Task [Transform]: Segments generated: {segments}")
    return segments


def _run_load_warehouse(**context):
    from src.config_loader import load_config
    from src.extractor import extract
    from src.transformer import transform
    from src.loader import load
    
    config = load_config()
    csv_path = ROOT_DIR / config["paths"]["input_csv"]
    db_path = ROOT_DIR / config["paths"]["database"]
    
    raw_df = extract(csv_path)
    transformed_df = transform(raw_df, rfm_config=config["rfm"], seg_config=config["segmentation"])
    run_id = load(transformed_df, db_path=db_path, table_config=config["warehouse"]["tables"])
    if run_id is None:
        raise RuntimeError(f"Warehouse load failed on database: {db_path}")
    print(f"Airflow Task [Load]: Warehouse run_id={run_id} completed.")
    return run_id


def _run_reverse_etl_push(**context):
    from src.config_loader import load_config
    from src.loader import read_from_warehouse
    from src.reverse_etl import run_reverse_etl
    
    config = load_config()
    db_path = ROOT_DIR / config["paths"]["database"]
    
    # Retrieve run_id or fallback to latest
    ti = context.get("ti")
    run_id = ti.xcom_pull(task_ids="load_warehouse_task") if ti else 1
    if not run_id:
        run_id = 1
        
    warehouse_df = read_from_warehouse(db_path, run_id=run_id)
    if warehouse_df is None or warehouse_df.empty:
        raise RuntimeError(f"No records found in warehouse for run_id={run_id}")
        
    summary = run_reverse_etl(
        warehouse_df=warehouse_df,
        crm_config=config["crm"],
        marketing_actions=config["marketing_actions"],
        use_bulk=True
    )
    print(f"Airflow Task [Reverse ETL Push]: CRM push results: {summary}")
    return summary


def _run_notify_and_audit(**context):
    ti = context.get("ti")
    run_id = ti.xcom_pull(task_ids="load_warehouse_task") if ti else "N/A"
    summary = ti.xcom_pull(task_ids="reverse_etl_crm_push_task") if ti else {}
    print("=" * 60)
    print("AIRFLOW PIPELINE EXECUTION COMPLETED")
    print(f"Warehouse Run ID : {run_id}")
    print(f"CRM Sync Summary : {summary}")
    print("=" * 60)
    return {"status": "SUCCESS", "run_id": run_id}


# Default arguments for Airflow DAG
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

if AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="reverse_etl_customer_segmentation_dag",
        default_args=default_args,
        description="Hourly DAG: Ingests customer data, segments by RFM, stores in warehouse, and pushes segments to CRM via Reverse ETL.",
        schedule_interval="0 * * * *",  # Hourly
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["reverse-etl", "rfm-segmentation", "crm-sync", "production"],
    )

    with dag:
        extract_task = PythonOperator(
            task_id="extract_and_validate_task",
            python_callable=_run_extract,
        )

        transform_task = PythonOperator(
            task_id="transform_and_segment_task",
            python_callable=_run_transform,
        )

        load_warehouse_task = PythonOperator(
            task_id="load_warehouse_task",
            python_callable=_run_load_warehouse,
        )

        reverse_etl_push_task = PythonOperator(
            task_id="reverse_etl_crm_push_task",
            python_callable=_run_reverse_etl_push,
        )

        notify_and_audit_task = PythonOperator(
            task_id="notify_and_audit_task",
            python_callable=_run_notify_and_audit,
        )

        # Sequential dependency pipeline
        extract_task >> transform_task >> load_warehouse_task >> reverse_etl_push_task >> notify_and_audit_task
else:
    # Airflow not installed on local host (e.g. Windows), but functions are fully defined and testable
    dag = None
