import great_expectations as gx
import pandas as pd
import psycopg2

# ── Connect to PostgreSQL ──────────────────────────────────────────
def get_df(query):
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname="transitflow_db",
        user="postgres",
        password="postgres"
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ── Load data ──────────────────────────────────────────────────────
print("📥 Loading data from PostgreSQL...")
routes_df  = get_df("SELECT * FROM analytics.stg_routes")
stops_df   = get_df("SELECT * FROM analytics.stg_stops")
trips_df   = get_df("SELECT * FROM analytics.stg_trips")
busiest_df = get_df("SELECT * FROM analytics.fct_busiest_routes")

print("\n🧪 Running Great Expectations validations...\n")

# ── Helper function ────────────────────────────────────────────────
def validate(name, df, expectations):
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(name=name)
    da = ds.add_dataframe_asset(name=f"{name}_asset")
    batch = da.add_batch_definition_whole_dataframe(f"{name}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{name}_suite"))

    for exp in expectations:
        suite.add_expectation(exp)

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"{name}_validation",
            data=batch,
            suite=suite
        )
    )
    results = vd.run(batch_parameters={"dataframe": df})
    passed  = sum(1 for r in results.results if r.success)
    total   = len(results.results)
    status  = "✅" if passed == total else "⚠️"
    print(f"{status} {name} validation: {passed}/{total} expectations passed")
    if passed != total:
        for r in results.results:
            if not r.success:
                print(f"   ❌ FAILED: {r.expectation_config.type}")

# ── VALIDATION 1: Routes ───────────────────────────────────────────
validate("routes", routes_df, [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="route_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="route_short_name"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="route_long_name"),
    gx.expectations.ExpectColumnValuesToBeUnique(column="route_id"),
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=100, max_value=500),
])

# ── VALIDATION 2: Stops ────────────────────────────────────────────
validate("stops", stops_df, [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="stop_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="stop_name"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="stop_lat"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="stop_lon"),
    gx.expectations.ExpectColumnValuesToBeUnique(column="stop_id"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="stop_lat", min_value=43.0, max_value=44.5),
    gx.expectations.ExpectColumnValuesToBeBetween(column="stop_lon", min_value=-80.0, max_value=-78.0),
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=5000, max_value=20000),
])

# ── VALIDATION 3: Trips ────────────────────────────────────────────
validate("trips", trips_df, [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="route_id"),
    gx.expectations.ExpectColumnValuesToBeUnique(column="trip_id"),
    gx.expectations.ExpectColumnValuesToBeInSet(column="direction_id", value_set=["0", "1"]),
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=50000, max_value=500000),
])

# ── VALIDATION 4: Busiest Routes ───────────────────────────────────
validate("busiest_routes", busiest_df, [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="route_short_name"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="total_trips"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="unique_stops"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="total_trips", min_value=1, max_value=10000),
    gx.expectations.ExpectColumnValuesToBeBetween(column="avg_stops_per_trip", min_value=1, max_value=200),
])

print("\n🎉 All Great Expectations validations complete!")