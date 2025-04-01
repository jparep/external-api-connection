# ingest_push_to_snowflake.py

import snowflake.connector
import pandas as pd
import requests
import hashlib
import json
import os
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# Establish connection using .env credentials
conn = snowflake.connector.connect(
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

def ingest_api_to_snowflake():
    cursor = conn.cursor()
    try:
        # Step 1: Fetch data
        api_url = "https://disease.sh/v3/covid-19/countries"
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        api_fingerprint = compute_hash(response.text)

        print("✅ API call succeeded.")

        # Step 2: Prepare data
        df = pd.json_normalize(json_data)
        df.columns = [c.upper().replace(" ", "_") for c in df.columns]
        df["_INGEST_TS"] = datetime.utcnow()
        df["RECORD_HASH"] = [compute_hash(rec) for rec in json_data]

        # Step 3: Filter out existing hashes
        existing_hashes = set()
        cursor.execute("SELECT RECORD_HASH FROM RAW.RAW_JSON.RECORD_HASH_TRACKER")
        for row in cursor.fetchall():
            existing_hashes.add(row[0])

        df_filtered = df[~df["RECORD_HASH"].isin(existing_hashes)]
        print(f"🔍 {len(df_filtered)} new records identified.")

        # Step 4: Insert raw JSON archive using bind parameters
        insert_sql = """
            INSERT INTO RAW.RAW_JSON.RAW_JSON_ARCHIVE
            (id, raw_payload, api_id, api_fingerprint, record_hash, status)
            VALUES (%s, PARSE_JSON(%s), %s, %s, %s, %s)
        """

        for i, row in df_filtered.iterrows():
            payload = json.dumps(json_data[i])
            cursor.execute(insert_sql, (
                row['COUNTRY'],
                payload,
                'covid_api',
                api_fingerprint,
                row['RECORD_HASH'],
                'fetched'
            ))

        # Step 5: Insert flattened data into table (auto-create if needed)
        table_name = "COVID_STATS_GENERIC"
        create_cols = []
        for col, dtype in df_filtered.dtypes.items():
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

        create_sql = f"CREATE TABLE IF NOT EXISTS RAW.RAW_JSON.{table_name} ({', '.join(create_cols)})"
        cursor.execute(create_sql)

        # Step 6: Write to flattened table
        success, nchunks, nrows, _ = snowflake.connector.pandas_tools.write_pandas(
            conn,
            df_filtered,
            table_name=table_name,
            schema="RAW_JSON",
            database="RAW",
            overwrite=False
        )
        print(f"📦 {nrows} flattened rows inserted into {table_name}")

        # Step 7: Insert hashes into tracker
        for rh in df_filtered["RECORD_HASH"].tolist():
            cursor.execute("""
                INSERT INTO RAW.RAW_JSON.RECORD_HASH_TRACKER (record_hash, record_source)
                VALUES (%s, %s)
            """, (rh, 'covid_api'))

        # Step 8: Log API fingerprint
        cursor.execute("""
            INSERT INTO RAW.RAW_JSON.API_FINGERPRINT_LOG (api_id, fingerprint, payload_length)
            VALUES (%s, %s, %s)
        """, ('covid_api', api_fingerprint, len(response.text)))

        print("✅ Ingestion and logging completed.")

    except Exception as e:
        print(f"❌ ERROR: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("🚀 Script started...")
    ingest_api_to_snowflake()
    