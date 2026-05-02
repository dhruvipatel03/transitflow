import requests
import zipfile
import io
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ─── Database Connection ───────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname="transitflow_db",
        user="postgres",
        password="postgres"
    )

# ─── Download GTFS Static Feed ─────────────────────────────────────
def download_gtfs():
    zip_path = os.path.join(os.path.dirname(__file__), "gtfs.zip")
    
    if not os.path.exists(zip_path):
        raise Exception("❌ gtfs.zip not found! Please place it in the ingestion/ folder.")
    
    print("📂 Loading GTFS feed from local file...")
    return zipfile.ZipFile(zip_path)
# ─── Create Raw Tables ─────────────────────────────────────────────
def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.routes (
            route_id        TEXT,
            agency_id       TEXT,
            route_short_name TEXT,
            route_long_name  TEXT,
            route_type      TEXT,
            loaded_at       TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.stops (
            stop_id     TEXT,
            stop_name   TEXT,
            stop_lat    FLOAT,
            stop_lon    FLOAT,
            loaded_at   TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.trips (
            route_id    TEXT,
            service_id  TEXT,
            trip_id     TEXT,
            trip_headsign TEXT,
            direction_id  TEXT,
            loaded_at   TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.stop_times (
            trip_id         TEXT,
            arrival_time    TEXT,
            departure_time  TEXT,
            stop_id         TEXT,
            stop_sequence   INTEGER,
            loaded_at       TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    cursor.close()
    print("✅ Raw tables created!")

# ─── Load CSV from ZIP into PostgreSQL ────────────────────────────
def load_csv_to_table(conn, zf, filename, table_name, columns):
    if filename not in zf.namelist():
        print(f"⚠️  {filename} not found in ZIP, skipping...")
        return

    print(f"📥 Loading {filename} → raw.{table_name}...")
    df = pd.read_csv(zf.open(filename), usecols=lambda c: c in columns, dtype=str)
    df = df[[c for c in columns if c in df.columns]]
    df = df.dropna(how='all')

    # Bulk insert using StringIO + COPY (100x faster than row by row)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE raw.{table_name};")
    
    col_names = ", ".join([c for c in columns if c in df.columns])
    cursor.copy_expert(
        f"COPY raw.{table_name} ({col_names}) FROM STDIN WITH CSV",
        buffer
    )

    conn.commit()
    cursor.close()
    print(f"✅ Loaded {len(df)} rows into raw.{table_name}")

# ─── Main ──────────────────────────────────────────────────────────
def main():
    zf   = download_gtfs()
    conn = get_connection()

    print("\n📋 Files inside the ZIP:")
    for name in zf.namelist():
        print(f"   {name}")

    create_tables(conn)

    load_csv_to_table(conn, zf, "routes.txt", "routes",
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"])

    load_csv_to_table(conn, zf, "stops.txt", "stops",
        ["stop_id", "stop_name", "stop_lat", "stop_lon"])

    load_csv_to_table(conn, zf, "trips.txt", "trips",
        ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"])

    load_csv_to_table(conn, zf, "stop_times.txt", "stop_times",
        ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])

    conn.close()
    print("\n🎉 All data loaded successfully into PostgreSQL!")

if __name__ == "__main__":
    main()