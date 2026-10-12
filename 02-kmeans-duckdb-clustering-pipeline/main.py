
"""
CUSTOMER SEGMENTATION & REVERSE ETL PIPELINE

Workflow:
1. Load and standardize the customer CSV
2. Validate and clean customer data
3. Store raw data in DuckDB
4. Encode features and scale numerical data
5. Perform K-Means clustering
6. Profile and name customer segments
7. Store segments in DuckDB
8. Export enriched customer records to a simulated CRM
9. Generate a customer segmentation visualization

Expected input columns:
ID, Gender, Ever_Married, Age, Graduated, Profession,
Work_Experience, Spending_Score, Family_Size, Var_1,
Segmentation

The existing Segmentation column is retained as source data,
but is NOT used as an input to K-Means.
"""

from pathlib import Path
from datetime import datetime, timezone
import warnings

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "customers.csv"
DATABASE_FILE = BASE_DIR / "customer_analytics.duckdb"
CRM_OUTPUT_FILE = BASE_DIR / "crm_customers.csv"
SEGMENT_OUTPUT_FILE = BASE_DIR / "customer_segments.csv"
PROFILE_OUTPUT_FILE = BASE_DIR / "segment_profiles.csv"
CHART_FILE = BASE_DIR / "customer_segments.png"

N_CLUSTERS = 5
RANDOM_STATE = 42

# Columns that are identifiers or existing labels are not
# used as K-Means features.
ID_COLUMN = "CustomerID"
SOURCE_LABEL_COLUMN = "Segmentation"

# These aliases allow the pipeline to work with multiple
# common customer dataset column naming conventions.
COLUMN_ALIASES = {
    "customerid": "CustomerID",
    "customer_id": "CustomerID",
    "id": "CustomerID",
    "annual_income": "Annual_Income",
    "annual income (k$)": "Annual_Income",
    "income": "Annual_Income",
    "spending_score": "Spending_Score",
    "spending score (1-100)": "Spending_Score",
    "work_experience": "Work_Experience",
    "family_size": "Family_Size",
    "ever_married": "Ever_Married",
    "graduated": "Graduated",
    "var_1": "Var_1",
}


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def print_section(title):
    """Print a readable section heading."""
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def standardize_column_names(df):
    """Standardize column names and recognize common aliases."""
    rename_map = {}

    for column in df.columns:
        original = str(column).strip()
        normalized = original.lower().strip()

        if normalized in COLUMN_ALIASES:
            rename_map[column] = COLUMN_ALIASES[normalized]
        else:
            rename_map[column] = original

    df = df.rename(columns=rename_map)

    # Avoid ambiguous duplicate column names after renaming.
    if df.columns.duplicated().any():
        duplicates = df.columns[df.columns.duplicated()].tolist()
        raise ValueError(
            f"Duplicate columns found after standardization: {duplicates}"
        )

    return df


# ============================================================
# 3. EXTRACT: LOAD AND STANDARDIZE CSV
# ============================================================

def load_and_standardize_csv():
    """Load the source CSV and standardize its column names."""

    print_section("STEP 1: EXTRACT CUSTOMER DATA")

    if not INPUT_FILE.exists():
        csv_files = list(BASE_DIR.glob("*.csv"))

        available_files = [
            file.name for file in csv_files
        ]

        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}\n"
            f"Place your dataset in the project directory and name it "
            f"'customers.csv'.\n"
            f"CSV files currently available: {available_files}"
        )

    print(f"Reading dataset: {INPUT_FILE.name}")

    df = pd.read_csv(INPUT_FILE)
    df = standardize_column_names(df)

    print(f"Rows loaded: {len(df)}")
    print(f"Columns found: {list(df.columns)}")

    # The provided dataset uses ID instead of CustomerID.
    if "CustomerID" not in df.columns and "ID" in df.columns:
        df = df.rename(columns={"ID": "CustomerID"})

    if "CustomerID" not in df.columns:
        raise ValueError(
            "Required column for CustomerID is missing.\n"
            f"Found columns: {list(df.columns)}\n"
            "Expected either 'CustomerID' or 'ID'."
        )

    # A stable identifier is required for Reverse ETL.
    df["CustomerID"] = df["CustomerID"].astype("string").str.strip()

    df["CustomerID"] = df["CustomerID"].replace("", pd.NA)

    if df["CustomerID"].isna().any():
        missing_count = int(df["CustomerID"].isna().sum())
        raise ValueError(
            f"{missing_count} rows have missing customer IDs. "
            "Correct the source CSV before continuing."
        )

    if df["CustomerID"].duplicated().any():
        duplicate_ids = (
            df.loc[
                df["CustomerID"].duplicated(keep=False),
                "CustomerID"
            ]
            .unique()
            .tolist()
        )

        raise ValueError(
            "Duplicate customer IDs were found. "
            f"Examples: {duplicate_ids[:10]}. "
            "Each customer must have a unique ID."
        )

    print("\nStandardized customer ID column successfully.")

    # Do not drop the original Segmentation column.
    # It is preserved for reference, but excluded from ML features.
    if SOURCE_LABEL_COLUMN in df.columns:
        print(
            "Existing 'Segmentation' column detected. "
            "It will be preserved but excluded from K-Means features."
        )

    print("\nStandardized columns:")
    for column in df.columns:
        print(f"  - {column}")

    return df


# ============================================================
# 4. VALIDATE AND CLEAN DATA
# ============================================================

def ingest_and_validate(df):
    """Validate records, clean invalid numeric values and persist raw data."""

    print_section("STEP 2: VALIDATE AND CLEAN DATA")

    df = df.copy()

    original_rows = len(df)

    # Convert known numerical fields to numeric types.
    numeric_candidates = [
        "Age",
        "Annual_Income",
        "Spending_Score",
        "Work_Experience",
        "Family_Size",
    ]

    for column in numeric_candidates:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # Negative values in these fields are not meaningful.
    for column in [
        "Age",
        "Annual_Income",
        "Spending_Score",
        "Work_Experience",
        "Family_Size",
    ]:
        if column in df.columns:
            invalid_mask = df[column] < 0
            invalid_count = int(invalid_mask.sum())

            if invalid_count:
                warnings.warn(
                    f"{invalid_count} negative values in {column} "
                    "will be converted to missing values."
                )
                df.loc[invalid_mask, column] = np.nan

    # An age of zero or an unrealistic age should not be used
    # as a valid age for this customer segmentation example.
    if "Age" in df.columns:
        invalid_age = (df["Age"] <= 0) | (df["Age"] > 120)

        if invalid_age.any():
            warnings.warn(
                f"{int(invalid_age.sum())} invalid Age values "
                "will be treated as missing."
            )
            df.loc[invalid_age, "Age"] = np.nan

    # Spending_Score commonly uses a 1-100 scale.
    if "Spending_Score" in df.columns:
        invalid_spending = (
            (df["Spending_Score"] < 1)
            | (df["Spending_Score"] > 100)
        )

        if invalid_spending.any():
            warnings.warn(
                f"{int(invalid_spending.sum())} Spending_Score values "
                "outside 1-100 will be treated as missing."
            )
            df.loc[invalid_spending, "Spending_Score"] = np.nan

    # Clean whitespace and represent blank categorical values as missing.
    # Convert to plain object columns with NumPy NaN, because pandas' pd.NA
    # values can trip scikit-learn's missing-value checks during preprocessing.
    for column in df.select_dtypes(
        include=["object", "string", "category"]
    ).columns:
        df[column] = df[column].astype("string").str.strip()
        df[column] = df[column].replace("", pd.NA)
        df[column] = df[column].astype(object)
        df[column] = df[column].where(df[column].notna(), np.nan)

    # Preserve records with incomplete features. The ML preprocessing
    # pipeline will impute missing feature values.
    if df[ID_COLUMN].isna().any():
        raise ValueError("CustomerID cannot be missing.")

    if df[ID_COLUMN].duplicated().any():
        raise ValueError("CustomerID must be unique.")

    print(f"Rows before validation: {original_rows}")
    print(f"Rows after validation:  {len(df)}")
    print(f"Missing values:         {int(df.isna().sum().sum())}")

    print("\nMissing values by column:")
    print(df.isna().sum().to_string())

    # Store the standardized source data in DuckDB.
    with duckdb.connect(str(DATABASE_FILE)) as con:
        con.register("source_dataframe", df)

        con.execute("""
            CREATE OR REPLACE TABLE raw_customers AS
            SELECT * FROM source_dataframe
        """)

        con.unregister("source_dataframe")

        stored_count = con.execute(
            "SELECT COUNT(*) FROM raw_customers"
        ).fetchone()[0]

    print(f"\nDuckDB database: {DATABASE_FILE.name}")
    print(f"Records stored in raw_customers: {stored_count}")

    return df


# ============================================================
# 5. FEATURE SELECTION AND K-MEANS CLUSTERING
# ============================================================

def perform_clustering(df):
    """Preprocess eligible features and assign K-Means clusters."""

    print_section("STEP 3: CUSTOMER SEGMENTATION WITH K-MEANS")

    df = df.copy()

    excluded_columns = {
        ID_COLUMN,
        SOURCE_LABEL_COLUMN,
        "Cluster",
        "SegmentName",
        "SegmentUpdatedAt",
    }

    # Automatically use the available customer attributes.
    feature_columns = [
        column
        for column in df.columns
        if column not in excluded_columns
    ]

    if not feature_columns:
        raise ValueError("No usable customer features were found.")

    # Exclude columns that are completely empty.
    feature_columns = [
        column
        for column in feature_columns
        if not df[column].isna().all()
    ]

    if not feature_columns:
        raise ValueError(
            "All candidate customer features are empty."
        )

    X = df[feature_columns].copy()

    # Separate numerical and categorical features.
    numeric_features = X.select_dtypes(
        include=["number"]
    ).columns.tolist()

    categorical_features = [
        column
        for column in X.columns
        if column not in numeric_features
    ]

    # Exclude constant numerical/categorical columns.
    numeric_features = [
        column
        for column in numeric_features
        if X[column].nunique(dropna=True) > 1
    ]

    categorical_features = [
        column
        for column in categorical_features
        if X[column].nunique(dropna=True) > 1
    ]

    feature_columns = numeric_features + categorical_features

    if not feature_columns:
        raise ValueError(
            "No varying numerical or categorical features remain."
        )

    X = X[feature_columns]

    if len(df) < 2:
        raise ValueError(
            "At least two customer records are required for clustering."
        )

    # K-Means requires at least as many records as clusters.
    actual_clusters = min(N_CLUSTERS, len(df))

    if actual_clusters < N_CLUSTERS:
        warnings.warn(
            f"Only {len(df)} customers are available. "
            f"Using {actual_clusters} clusters instead of {N_CLUSTERS}."
        )

    transformers = []

    if numeric_features:
        numeric_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="median")
                ),
                (
                    "scaler",
                    StandardScaler()
                ),
            ]
        )

        transformers.append(
            ("numeric", numeric_pipeline, numeric_features)
        )

    if categorical_features:
        categorical_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="most_frequent")
                ),
                (
                    "encoder",
                    OneHotEncoder(
                        handle_unknown="ignore"
                    )
                ),
            ]
        )

        transformers.append(
            ("categorical", categorical_pipeline, categorical_features)
        )

    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder="drop",
    )

    print(f"Customer records: {len(df)}")
    print(f"Numerical features: {numeric_features}")
    print(f"Categorical features: {categorical_features}")
    print(f"Features excluded from clustering: {sorted(excluded_columns)}")
    print(f"Number of clusters: {actual_clusters}")

    # Fit transformations using customer attributes only.
    X_processed = preprocessor.fit_transform(X)

    # K-Means expects a finite feature matrix.
    if hasattr(X_processed, "toarray"):
        X_processed = X_processed.toarray()

    X_processed = np.asarray(X_processed, dtype=float)

    if not np.isfinite(X_processed).all():
        raise ValueError(
            "The preprocessed feature matrix contains NaN or infinite "
            "values. Check the source dataset."
        )

    model = KMeans(
        n_clusters=actual_clusters,
        random_state=RANDOM_STATE,
        n_init=10,
    )

    df["Cluster"] = model.fit_predict(X_processed)

    print("\nClustering completed successfully.")
    print("\nCustomer count by cluster:")
    print(
        df["Cluster"]
        .value_counts()
        .sort_index()
        .to_string()
    )

    return df, X_processed, model, feature_columns


# ============================================================
# 6. PROFILE AND NAME CUSTOMER SEGMENTS
# ============================================================

def profile_segments(df, feature_columns):
    """Create interpretable profiles using customer attributes."""

    print_section("STEP 4: PROFILE CUSTOMER SEGMENTS")

    df = df.copy()

    if "Cluster" not in df.columns:
        raise ValueError("Cluster assignments are missing.")

    # Summarize numeric features for interpretation.
    numeric_features = [
        column
        for column in feature_columns
        if column in df.columns
        and pd.api.types.is_numeric_dtype(df[column])
    ]

    profiles = (
        df.groupby("Cluster")
        .agg(
            Customer_Count=("CustomerID", "count"),
            **{
                f"Average_{column}": (column, "mean")
                for column in numeric_features
            },
        )
        .reset_index()
    )

    # Use the available Spending_Score and Age features to produce
    # human-readable labels. If those fields are absent, use
    # neutral cluster-based names.
    if "Spending_Score" in df.columns:
        spending_mean = df["Spending_Score"].mean()
        spending_std = df["Spending_Score"].std()

        if pd.isna(spending_std) or spending_std == 0:
            spending_std = 1.0
    else:
        spending_mean = None
        spending_std = None

    if "Age" in df.columns:
        age_mean = df["Age"].mean()
        age_std = df["Age"].std()

        if pd.isna(age_std) or age_std == 0:
            age_std = 1.0
    else:
        age_mean = None
        age_std = None

    segment_names = {}

    for _, row in profiles.iterrows():
        cluster_id = int(row["Cluster"])
        cluster_df = df[df["Cluster"] == cluster_id]

        spending_label = None
        age_label = None

        if spending_mean is not None:
            cluster_spending = cluster_df["Spending_Score"].mean()

            if pd.notna(cluster_spending):
                if cluster_spending >= spending_mean + 0.35 * spending_std:
                    spending_label = "High Spenders"
                elif cluster_spending <= spending_mean - 0.35 * spending_std:
                    spending_label = "Low Spenders"
                else:
                    spending_label = "Moderate Spenders"

        if age_mean is not None:
            cluster_age = cluster_df["Age"].mean()

            if pd.notna(cluster_age):
                if cluster_age < age_mean - 0.35 * age_std:
                    age_label = "Younger Customers"
                elif cluster_age > age_mean + 0.35 * age_std:
                    age_label = "Older Customers"

        if spending_label and age_label:
            segment_name = f"{age_label} - {spending_label}"
        elif spending_label:
            segment_name = spending_label
        elif age_label:
            segment_name = age_label
        else:
            segment_name = f"Customer Segment {cluster_id}"

        segment_names[cluster_id] = segment_name

    df["SegmentName"] = df["Cluster"].map(segment_names)

    # Include the final segment name in the profile output.
    profiles["SegmentName"] = profiles["Cluster"].map(segment_names)

    print("\nSegment profiles:")
    print(profiles.to_string(index=False))

    print("\nCustomer segment assignments:")
    print(
        df[
            ["CustomerID", "Cluster", "SegmentName"]
        ]
        .head(10)
        .to_string(index=False)
    )

    return df, profiles


# ============================================================
# 7. LOAD RESULTS INTO DUCKDB AND EXPORT TO SIMULATED CRM
# ============================================================

def store_and_export(df, profiles):
    """Persist clustering results and perform the Reverse ETL export."""

    print_section("STEP 5: DUCKDB STORAGE AND REVERSE ETL")

    df = df.copy()
    profiles = profiles.copy()

    # Add an export timestamp to support downstream CRM synchronization.
    export_timestamp = datetime.now(timezone.utc).isoformat()
    df["SegmentUpdatedAt"] = export_timestamp

    # Create a CRM-friendly output schema.
    # Preserve the original customer attributes while adding
    # standardized ID and predicted segmentation fields.
    crm_df = df.copy()

    # Rename the generated segment to make its origin explicit.
    crm_df = crm_df.rename(
        columns={
            "Cluster": "PredictedCluster",
            "SegmentName": "PredictedSegment",
        }
    )

    # Never overwrite the source Segmentation field with model output.
    # The two columns represent different things.
    if SOURCE_LABEL_COLUMN in crm_df.columns:
        print(
            "Keeping original Segmentation labels separate from "
            "PredictedSegment."
        )

    # Export the enriched customer data for the simulated CRM.
    crm_df.to_csv(
        CRM_OUTPUT_FILE,
        index=False,
        encoding="utf-8",
    )

    profiles.to_csv(
        PROFILE_OUTPUT_FILE,
        index=False,
        encoding="utf-8",
    )

    # Store the results in DuckDB.
    with duckdb.connect(str(DATABASE_FILE)) as con:
        con.register("segmented_dataframe", df)
        con.register("profiles_dataframe", profiles)

        con.execute("""
            CREATE OR REPLACE TABLE customer_segments AS
            SELECT * FROM segmented_dataframe
        """)

        con.execute("""
            CREATE OR REPLACE TABLE segment_profiles AS
            SELECT * FROM profiles_dataframe
        """)

        con.unregister("segmented_dataframe")
        con.unregister("profiles_dataframe")

        # Verify the exported data.
        database_count = con.execute(
            "SELECT COUNT(*) FROM customer_segments"
        ).fetchone()[0]

        unique_customer_count = con.execute("""
            SELECT COUNT(DISTINCT CustomerID)
            FROM customer_segments
        """).fetchone()[0]

        cluster_summary = con.execute("""
            SELECT
                Cluster,
                SegmentName,
                COUNT(*) AS Customer_Count
            FROM customer_segments
            GROUP BY Cluster, SegmentName
            ORDER BY Cluster
        """).df()

    # Validate Reverse ETL export.
    exported_df = pd.read_csv(
        CRM_OUTPUT_FILE,
        dtype={"CustomerID": "string"},
    )

    exported_count = len(exported_df)

    if exported_count != len(df):
        raise RuntimeError(
            f"Reverse ETL row-count mismatch: "
            f"expected {len(df)}, exported {exported_count}."
        )

    if exported_df["CustomerID"].duplicated().any():
        raise RuntimeError(
            "Reverse ETL validation failed: duplicate CustomerID "
            "values found in the CRM export."
        )

    if exported_df["PredictedCluster"].isna().any():
        raise RuntimeError(
            "Reverse ETL validation failed: missing predicted clusters."
        )

    if exported_df["PredictedSegment"].isna().any():
        raise RuntimeError(
            "Reverse ETL validation failed: missing predicted segments."
        )

    print(f"DuckDB customer_segments rows: {database_count}")
    print(f"Unique customers in DuckDB:    {unique_customer_count}")
    print(f"CRM export rows:               {exported_count}")

    print("\nCRM destination schema:")
    print(list(exported_df.columns))

    print("\nReverse ETL segment summary:")
    print(cluster_summary.to_string(index=False))

    print("\nOutput files:")
    print(f"  Database:        {DATABASE_FILE.name}")
    print(f"  CRM export:      {CRM_OUTPUT_FILE.name}")
    print(f"  Segment export:  {SEGMENT_OUTPUT_FILE.name}")
    print(f"  Profile report:  {PROFILE_OUTPUT_FILE.name}")

    return crm_df


# ============================================================
# 8. VISUALIZE CUSTOMER SEGMENTS
# ============================================================

def generate_visualization(df, X_processed):
    """Generate a two-dimensional PCA plot of customer clusters."""

    print_section("STEP 6: GENERATE VISUALIZATION")

    df = df.copy()

    if "Cluster" not in df.columns:
        raise ValueError("Cannot visualize data without cluster labels.")

    X_processed = np.asarray(X_processed, dtype=float)

    if X_processed.ndim != 2 or X_processed.shape[0] != len(df):
        raise ValueError(
            "The processed feature matrix does not match the customer data."
        )

    if X_processed.shape[1] == 0:
        raise ValueError("No processed features are available to visualize.")

    # PCA provides two coordinates for a visual representation.
    if X_processed.shape[1] >= 2 and len(df) >= 2:
        pca = PCA(n_components=2, random_state=RANDOM_STATE)
        coordinates = pca.fit_transform(X_processed)

        x_label = "Principal Component 1"
        y_label = "Principal Component 2"

        explained = pca.explained_variance_ratio_.sum() * 100
        subtitle = f"Two-component PCA ({explained:.1f}% variance explained)"

    elif X_processed.shape[1] == 1:
        coordinates = np.column_stack(
            [X_processed[:, 0], np.zeros(len(df))]
        )

        x_label = "Processed Feature"
        y_label = "Placeholder Axis"
        subtitle = "One-dimensional feature representation"

    else:
        raise ValueError(
            "Not enough customer records to create a useful visualization."
        )

    fig, ax = plt.subplots(figsize=(11, 7))

    clusters = sorted(df["Cluster"].unique())

    for cluster_id in clusters:
        mask = df["Cluster"].to_numpy() == cluster_id

        cluster_name = df.loc[
            mask, "SegmentName"
        ].iloc[0]

        ax.scatter(
            coordinates[mask, 0],
            coordinates[mask, 1],
            label=f"Cluster {cluster_id}: {cluster_name}",
            alpha=0.75,
            s=45,
        )

    ax.set_title(
        "Customer Segmentation Using K-Means\n"
        + subtitle,
        fontsize=14,
    )

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    ax.grid(True, linestyle="--", alpha=0.25)
    ax.legend(
        title="Customer segments",
        fontsize=8,
        loc="best",
    )

    fig.tight_layout()
    fig.savefig(
        CHART_FILE,
        dpi=300,
        bbox_inches="tight",
    )

    plt.close(fig)

    print(f"Visualization saved: {CHART_FILE.name}")


# ============================================================
# 9. MAIN PIPELINE ORCHESTRATION
# ============================================================

def main():
    """Execute the end-to-end customer segmentation pipeline."""

    print("\nCUSTOMER SEGMENTATION & REVERSE ETL PIPELINE")

    start_time = datetime.now()

    try:
        # Extract
        raw_df = load_and_standardize_csv()

        # Validate, clean and load raw records into DuckDB
        clean_df = ingest_and_validate(raw_df)

        # Transform and cluster
        segmented_df, processed_features, model, feature_columns = (
            perform_clustering(clean_df)
        )

        # Profile the clusters
        segmented_df, profiles = profile_segments(
            segmented_df,
            feature_columns,
        )

        # Load segment data and export to simulated CRM
        store_and_export(segmented_df, profiles)

        # Visualize the clustering result
        generate_visualization(
            segmented_df,
            processed_features,
        )

        elapsed = (datetime.now() - start_time).total_seconds()

        print_section("PIPELINE COMPLETED SUCCESSFULLY")

        print(f"Total customers processed: {len(segmented_df)}")
        print(f"Clusters created: {segmented_df['Cluster'].nunique()}")
        print(f"Elapsed time: {elapsed:.2f} seconds")

        print("\nGenerated deliverables:")
        print(f"  1. {DATABASE_FILE.name}")
        print(f"  2. {CRM_OUTPUT_FILE.name}")
        print(f"  3. {SEGMENT_OUTPUT_FILE.name}")
        print(f"  4. {PROFILE_OUTPUT_FILE.name}")
        print(f"  5. {CHART_FILE.name}")

        print(
            "\nReverse ETL simulation complete: enriched customer "
            "records have been exported to the CRM CSV destination."
        )

    except Exception as exc:
        print_section("PIPELINE FAILED")
        print(f"Error type: {type(exc).__name__}")
        print(f"Error message: {exc}")
        raise


if __name__ == "__main__":
    main()
