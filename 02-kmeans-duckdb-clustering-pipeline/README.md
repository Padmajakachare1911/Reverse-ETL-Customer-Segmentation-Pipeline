# Customer Segmentation & Reverse ETL Pipeline (K-Means + DuckDB)

A modern, machine learning-driven **Reverse ETL pipeline** that extracts customer behavioral and demographic data, trains an unsupervised **K-Means clustering** model, profiles target customer personas, stores analytical states in an embedded columnar **DuckDB** warehouse, and **reverse-syncs enriched segment intelligence to operational CRM systems**.

---

## 🏗️ Architecture & Data Flow

```
                      CUSTOMER DATA INGESTION
                                 │
                                 ▼ [Step 1: Extract & Standardize]
                      Standardize column names & aliases
                                 │
                                 ▼ [Step 2: Clean & Validate]
                      Validate types, handle nulls
                                 │
                      ┌──────────┴──────────┐
                      │                     ▼
                      │       [DuckDB Analytical Warehouse]
                      │       - Table: raw_customers
                      │
                      ▼ [Step 3: ML Feature Preprocessing]
             ColumnTransformer (OneHotEncoder + StandardScaler + SimpleImputer)
                                 │
                                 ▼ [Step 4: Unsupervised Clustering]
             K-Means (k=5) + PCA (2D Dimensionality Reduction)
                                 │
                                 ▼ [Step 5: Segment Profiling]
             Profile metrics: Age, Spending Score, Profession, Family Size
             Generate Business Segment Personas
                                 │
                      ┌──────────┴──────────┐
                      ▼                     ▼
        [DuckDB Storage]           [Reverse ETL Operational Sync]
        - Table: customer_segments ├─ crm_customers.csv (Enriched CRM Payload)
        - Table: segment_profiles  ├─ segment_profiles.csv (Segment Metrics)
                                   └─ customer_segments.png (PCA Visual Map)
```

---

## 🎯 What Makes This Practical Unique?

| Core Component | Implementation in This Practical |
| :--- | :--- |
| **Segmentation Method** | **Unsupervised Machine Learning (K-Means Clustering, k=5)** with Scikit-Learn |
| **Feature Engineering** | Dynamic **ColumnTransformer**, **PCA Dimensionality Reduction**, **StandardScaler**, **OneHotEncoder** |
| **Analytical Warehouse** | **DuckDB** embedded columnar OLAP engine (`customer_analytics.duckdb`) |
| **Reverse ETL Target** | Operational CRM sync (`crm_customers.csv`) ready for downstream marketing/sales ingestion |
| **Visual Analytics** | Matplotlib 2D PCA cluster projection (`customer_segments.png`) |

---

## 📂 Project Directory Structure

```
02-kmeans-duckdb-clustering-pipeline/
├── customers.csv               # Input dataset (demographic & behavioral records)
├── main.py                     # End-to-end pipeline script
├── requirements.txt            # Python dependencies (duckdb, pandas, scikit-learn, matplotlib)
├── customer_analytics.duckdb   # DuckDB analytical warehouse (generated)
├── crm_customers.csv           # Enriched CRM sync target (generated)
├── customer_segments.csv       # Customer segment mappings (generated)
├── segment_profiles.csv        # Segment summary and persona metrics (generated)
├── customer_segments.png       # 2D PCA cluster visualization plot (generated)
└── README.md                   # This detailed practical guide
```

---

## 📊 Dataset Schema (`customers.csv`)

The input dataset contains customer records with demographic, lifestyle, and spending attributes:

| Column | Type | Description |
| :--- | :--- | :--- |
| `CustomerID` | Integer | Unique identifier for each customer |
| `Gender` | String | Male / Female |
| `Ever_Married` | String | Marital status indicator (Yes / No) |
| `Age` | Integer | Age of customer |
| `Graduated` | String | Education graduation status (Yes / No) |
| `Profession` | String | Occupation (Artist, Healthcare, Engineer, Lawyer, etc.) |
| `Work_Experience`| Float | Total years of professional work experience |
| `Spending_Score` | String | Spending habit classification (Low, Average, High) |
| `Family_Size` | Float | Number of family members |
| `Var_1` | String | Categorical lifestyle category code |
| `Segmentation` | String | Original baseline segment label (preserved for audit) |

---

## ⚙️ Step-by-Step Technical Execution

### Step 1: Ingestion & Schema Normalization
- Ingests `customers.csv` using Pandas.
- Normalizes column names (case-insensitive mapping and alias resolution for keys like `customer_id`, `income`, `work_experience`).
- Preserves original dataset labels separately from predicted ML clusters.

### Step 2: Data Cleaning & DuckDB Raw Staging
- Assesses missing values across features.
- Connects to embedded **DuckDB** instance (`customer_analytics.duckdb`).
- Loads cleaned raw records into table `raw_customers` using DuckDB's zero-copy columnar ingestion.

### Step 3: Feature Preprocessing Pipeline
- Separates numerical and categorical features:
  - **Numerical Features** (`Age`, `Work_Experience`, `Family_Size`): Imputed with median strategy and scaled via `StandardScaler()`.
  - **Categorical Features** (`Gender`, `Ever_Married`, `Graduated`, `Profession`, `Var_1`): Imputed with most frequent strategy and one-hot encoded via `OneHotEncoder(handle_unknown="ignore")`.
- Bundled into a scikit-learn `ColumnTransformer`.

### Step 4: K-Means Clustering & Dimensionality Reduction
- Fits a `KMeans(n_clusters=5, random_state=42)` model onto the transformed feature space.
- Performs **Principal Component Analysis (PCA)** with 2 components to reduce high-dimensional feature vectors for 2D visualization.
- Labels records with predicted cluster IDs (`0`, `1`, `2`, `3`, `4`).

### Step 5: Automated Cluster Profiling & Persona Labeling
- Aggregates mean statistics per cluster (Average Age, Work Experience, Family Size).
- Automatically assigns meaningful business persona names:
  - **Younger Customers** (Cluster 1: Avg age ~25, family size ~4.4)
  - **Older Customers** (Cluster 3: Avg age ~70, family size ~1.9)
  - **Balanced Working Professionals** (Clusters 0, 2, 4)

### Step 6: Analytical Persistence in DuckDB
- Writes customer segment assignments to DuckDB table `customer_segments`.
- Writes cluster metrics to DuckDB table `segment_profiles`.
- Ensures transactional integrity in the OLAP engine.

### Step 7: Reverse ETL Operational CRM Sync
- Queries enriched customer segment data directly from DuckDB.
- Formats payload according to downstream CRM schema requirements.
- Syncs output to `crm_customers.csv` and `segment_profiles.csv`.
- Generates a PCA scatter plot visualization saved to `customer_segments.png`.

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites
Ensure Python 3.9+ is installed.

### 2. Activate Virtual Environment & Install Requirements
From the project root:
```powershell
# Using the existing virtual environment:
..\.venv\Scripts\python.exe -m pip install -r requirements.txt
```
Or with standard Python:
```bash
python -m venv .venv
source .venv/bin/activate  # On Linux/macOS
# or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 3. Run the Pipeline
```bash
python main.py
```

### 4. Verify the Outputs
Inspect generated deliverables:
1. **`customer_analytics.duckdb`**: Columnar database containing `raw_customers`, `customer_segments`, and `segment_profiles`.
2. **`crm_customers.csv`**: Enriched customer records exported for CRM consumption.
3. **`segment_profiles.csv`**: Aggregated cluster metrics.
4. **`customer_segments.png`**: High-resolution PCA scatter plot showing cluster distribution.

---

## 🔍 Inspecting the DuckDB Analytical Database

You can query the embedded DuckDB warehouse using Python or DuckDB CLI:

```python
import duckdb

con = duckdb.connect("customer_analytics.duckdb")
print("Tables in DuckDB:")
print(con.execute("SHOW TABLES;").df())

print("\nSample Enriched Customer Segments:")
print(con.execute("""
    SELECT CustomerID, Age, Profession, PredictedCluster, PredictedSegment 
    FROM customer_segments 
    LIMIT 10;
""").df())

con.close()
```
