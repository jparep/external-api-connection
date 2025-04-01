import os
import json
import pandas as pd
import requests
import hashlib
from datetime import datetime
from dotenv import load_dotenv
from snowflake.snowpark import Session

# ✅ Patch the connector manually so write_pandas doesn't fail
import snowflake.connector.pandas_tools as sfpt
sfpt.pd = pd  # Force-load pandas into Snowflake connector space

# Load .env configs
load_dotenv()

# Snowflake connection config
connection_parameters = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Initialize Snowpark session
session = Session.builder.configs(connection_parameters).create()

def compute_hash(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()

def ingest():
    print("🚀 Starting Snowpark-based ingestion...")

    # Step 1: API Call
    api_url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    data = data[:5]  # ✅ Limit for testing

    df = pd.json_normalize(data)
    df.columns = [c.upper().replace(" ", "_") for c in df.columns]
    df["_INGEST_TS"] = datetime.utcnow()
    df["RECORD_HASH"] = [compute_hash(record) for record in data]

    print(f"✅ Retrieved {len(df)} records.")

    # Step 2: Write to Snowflake via Snowpark (auto-create table)
    table_name = "COVID_STATS_SNOWPARK"
    session.write_pandas(
        df,
        table_name=table_name,
        auto_create_table=True,
        overwrite=False
    )

    print(f"📦 Inserted {len(df)} records into {table_name}.")
    print("✅ Snowpark ingestion complete.")

if __name__ == "__main__":
    ingest()
