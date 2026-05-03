SELECT
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT t.trip_id)   AS total_trips,
    COUNT(DISTINCT st.stop_id)  AS unique_stops,
    COUNT(st.trip_id)           AS total_stop_events,
    ROUND(
        COUNT(st.trip_id)::numeric / 
        NULLIF(COUNT(DISTINCT t.trip_id), 0), 2
    )                           AS avg_stops_per_trip
FROM {{ ref('stg_routes') }}     r
LEFT JOIN {{ ref('stg_trips') }}      t  ON r.route_id  = t.route_id
LEFT JOIN {{ ref('stg_stop_times') }} st ON t.trip_id   = st.trip_id
GROUP BY r.route_short_name, r.route_long_name, r.route_type
ORDER BY total_trips DESC