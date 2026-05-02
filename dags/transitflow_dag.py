from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# ── Default arguments ──────────────────────────────────────────────
default_args = {
    'owner': 'transitflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── Ingestion function ─────────────────────────────────────────────
def run_ingestion():
    import zipfile
    import io
    import pandas as pd
    import psycopg2

    def get_connection():
        return psycopg2.connect(
            host="transitflow_postgres",
            port=5432,
            dbname="transitflow_db",
            user="postgres",
            password="postgres"
        )

    def create_tables(conn):
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.routes (
                route_id TEXT, agency_id TEXT,
                route_short_name TEXT, route_long_name TEXT,
                route_type TEXT, loaded_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS raw.stops (
                stop_id TEXT, stop_name TEXT,
                stop_lat FLOAT, stop_lon FLOAT,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS raw.trips (
                route_id TEXT, service_id TEXT, trip_id TEXT,
                trip_headsign TEXT, direction_id TEXT,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS raw.stop_times (
                trip_id TEXT, arrival_time TEXT, departure_time TEXT,
                stop_id TEXT, stop_sequence INTEGER,
                loaded_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cursor.close()

    def load_csv(conn, zf, filename, table, columns):
        if filename not in zf.namelist():
            return
        df = pd.read_csv(zf.open(filename), usecols=lambda c: c in columns, dtype=str)
        df = df[[c for c in columns if c in df.columns]].dropna(how='all')
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE raw.{table};")
        cur.copy_expert(f"COPY raw.{table} ({','.join([c for c in columns if c in df.columns])}) FROM STDIN WITH CSV", buf)
        conn.commit()
        cur.close()
        print(f"✅ Loaded {len(df)} rows into raw.{table}")

    zip_path = "/opt/airflow/dags/gtfs.zip"
    zf   = zipfile.ZipFile(zip_path)
    conn = get_connection()
    create_tables(conn)
    load_csv(conn, zf, "routes.txt",    "routes",     ["route_id","agency_id","route_short_name","route_long_name","route_type"])
    load_csv(conn, zf, "stops.txt",     "stops",      ["stop_id","stop_name","stop_lat","stop_lon"])
    load_csv(conn, zf, "trips.txt",     "trips",      ["route_id","service_id","trip_id","trip_headsign","direction_id"])
    load_csv(conn, zf, "stop_times.txt","stop_times", ["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    conn.close()
    print("🎉 Ingestion complete!")

# ── DAG definition ─────────────────────────────────────────────────
with DAG(
    dag_id='transitflow_pipeline',
    default_args=default_args,
    description='ETL pipeline for TTC transit data',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['transitflow', 'etl', 'transit'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_gtfs_data',
        python_callable=run_ingestion,
    )

    dbt_task = BashOperator(
        task_id='run_dbt_models',
        bash_command='echo "dbt run would execute here"',
    )

    ingest_task >> dbt_task