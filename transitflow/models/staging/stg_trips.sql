SELECT
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    direction_id,
    loaded_at
FROM raw.trips
WHERE trip_id IS NOT NULL