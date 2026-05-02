SELECT
    r.route_short_name,
    r.route_long_name,
    COUNT(DISTINCT st.stop_id)  AS total_stops,
    COUNT(DISTINCT st.trip_id)  AS total_trips
FROM {{ ref('stg_stop_times') }} st
LEFT JOIN {{ ref('stg_trips') }}  t ON st.trip_id  = t.trip_id
LEFT JOIN {{ ref('stg_routes') }} r ON t.route_id  = r.route_id
GROUP BY r.route_short_name, r.route_long_name
ORDER BY total_trips DESC