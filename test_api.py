import os
import json
import pandas as pd
import requests
import hashlib
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector import connect
import tempfile

# Load environment variables
load_dotenv()

# Snowflake credentials
conn = connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

def compute_hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()

def ingest():
    print("🚀 Starting CSV-based Snowflake ingestion...")

    # Step 1: Fetch API data
    api_url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()[:5]  # ✅ limit to 5 for testing

    df = pd.json_normalize(data)
    df.columns = [c.upper().replace(" ", "_") for c in df.columns]
    df["_INGEST_TS"] = datetime.utcnow()
    df["RECORD_HASH"] = [compute_hash(record) for record in data]

    table_name = "COVID_STATS_CSV"

    # Step 2: Save to local temp CSV
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmpfile:
        df.to_csv(tmpfile.name, index=False)
        local_csv = tmpfile.name

    # Step 3: Create table if not exists
    create_cols = []
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "NUMBER"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "TIMESTAMP"
        else:
            sql_type = "STRING"
        create_cols.append(f'"{col}" {sql_type}')
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(create_cols)})"

    cursor = conn.cursor()
    cursor.execute(create_sql)

    # Step 4: Upload file to Snowflake stage
    stage_name = f"@%{table_name}"
    put_sql = f"PUT file://{local_csv} {stage_name} OVERWRITE = TRUE"
    cursor.execute(put_sql)

    # Step 5: Load into table
    copy_sql = f"""
    COPY INTO {table_name}
    FROM {stage_name}
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
    """
    cursor.execute(copy_sql)

    print(f"✅ Loaded {len(df)} records into {table_name} using CSV + COPY INTO.")
    os.remove(local_csv)

if __name__ == "__main__":
    ingest()
