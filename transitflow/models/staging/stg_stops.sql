SELECT
    stop_id,
    stop_name,
    CAST(stop_lat AS FLOAT) AS stop_lat,
    CAST(stop_lon AS FLOAT) AS stop_lon,
    loaded_at
FROM raw.stops
WHERE stop_id IS NOT NULL