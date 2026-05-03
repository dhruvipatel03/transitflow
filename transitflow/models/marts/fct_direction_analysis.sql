SELECT
    r.route_short_name,
    r.route_long_name,
    t.direction_id,
    CASE 
        WHEN t.direction_id = '0' THEN 'Outbound'
        WHEN t.direction_id = '1' THEN 'Inbound'
        ELSE 'Unknown'
    END                             AS direction_label,
    t.trip_headsign,
    COUNT(DISTINCT t.trip_id)       AS total_trips,
    COUNT(DISTINCT st.stop_id)      AS unique_stops
FROM {{ ref('stg_trips') }}       t
LEFT JOIN {{ ref('stg_routes') }}      r  ON t.route_id = r.route_id
LEFT JOIN {{ ref('stg_stop_times') }} st  ON t.trip_id  = st.trip_id
WHERE t.direction_id IS NOT NULL
GROUP BY
    r.route_short_name, r.route_long_name,
    t.direction_id, t.trip_headsign
ORDER BY total_trips DESC