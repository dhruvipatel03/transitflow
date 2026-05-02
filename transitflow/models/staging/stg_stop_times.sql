SELECT
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    departure_time,
    loaded_at
FROM raw.stop_times
WHERE trip_id IS NOT NULL
AND stop_id IS NOT NULL