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
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.environ.get("POSTGRES_PORT", 5433)),
        dbname=os.environ.get("POSTGRES_DB", "transitflow_db"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres")
    )

# ─── Generate synthetic GTFS data for CI ──────────────────────────
def generate_synthetic_gtfs(zip_path):
    print("🔧 Generating synthetic GTFS data for CI...")

    routes = pd.DataFrame({
        "route_id": [f"R{i}" for i in range(1, 11)],
        "agency_id": ["TTC"] * 10,
        "route_short_name": [str(i) for i in range(1, 11)],
        "route_long_name": [f"Route {i} Long Name" for i in range(1, 11)],
        "route_type": ["3"] * 10
    })

    stops = pd.DataFrame({
        "stop_id": [f"S{i}" for i in range(1, 51)],
        "stop_name": [f"Stop {i}" for i in range(1, 51)],
        "stop_lat": [43.6 + i * 0.01 for i in range(50)],
        "stop_lon": [-79.4 - i * 0.01 for i in range(50)]
    })

    trips = pd.DataFrame({
        "route_id": [f"R{(i % 10) + 1}" for i in range(100)],
        "service_id": ["WD"] * 100,
        "trip_id": [f"T{i}" for i in range(1, 101)],
        "trip_headsign": [f"Headsign {i}" for i in range(1, 101)],
        "direction_id": [str(i % 2) for i in range(100)]
    })

    stop_times_rows = []
    for trip_idx in range(1, 101):
        for stop_seq in range(1, 11):
            stop_times_rows.append({
                "trip_id": f"T{trip_idx}",
                "arrival_time": f"{6 + stop_seq}:00:00",
                "departure_time": f"{6 + stop_seq}:01:00",
                "stop_id": f"S{stop_seq}",
                "stop_sequence": stop_seq
            })
    stop_times = pd.DataFrame(stop_times_rows)

    with zipfile.ZipFile(zip_path, 'w') as zf:
        for filename, df in [
            ("routes.txt", routes),
            ("stops.txt", stops),
            ("trips.txt", trips),
            ("stop_times.txt", stop_times)
        ]:
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            zf.writestr(filename, buf.getvalue())

    print("✅ Synthetic GTFS data generated!")

# ─── Load GTFS data ────────────────────────────────────────────────
def download_gtfs():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    zip_path = os.path.join(script_dir, "gtfs.zip")

    # Use real data if available
    if os.path.exists(zip_path) and zipfile.is_zipfile(zip_path):
        print(f"📂 Found valid GTFS at {zip_path}")
        return zipfile.ZipFile(zip_path)

    # Generate synthetic data for CI
    print("⚠️  No GTFS file found — generating synthetic data for CI...")
    generate_synthetic_gtfs(zip_path)
    return zipfile.ZipFile(zip_path)

# ─── Create Raw Tables ─────────────────────────────────────────────
def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.routes (
            route_id         TEXT,
            agency_id        TEXT,
            route_short_name TEXT,
            route_long_name  TEXT,
            route_type       TEXT,
            loaded_at        TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.stops (
            stop_id   TEXT,
            stop_name TEXT,
            stop_lat  FLOAT,
            stop_lon  FLOAT,
            loaded_at TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.trips (
            route_id      TEXT,
            service_id    TEXT,
            trip_id       TEXT,
            trip_headsign TEXT,
            direction_id  TEXT,
            loaded_at     TIMESTAMP DEFAULT NOW()
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.stop_times (
            trip_id        TEXT,
            arrival_time   TEXT,
            departure_time TEXT,
            stop_id        TEXT,
            stop_sequence  INTEGER,
            loaded_at      TIMESTAMP DEFAULT NOW()
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