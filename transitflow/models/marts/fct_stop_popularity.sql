SELECT
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    COUNT(st.trip_id)               AS total_visits,
    COUNT(DISTINCT t.route_id)      AS routes_serving_stop,
    MIN(st.arrival_time)            AS earliest_arrival,
    MAX(st.departure_time)          AS latest_departure
FROM {{ ref('stg_stops') }}        s
LEFT JOIN {{ ref('stg_stop_times') }} st ON s.stop_id  = st.stop_id
LEFT JOIN {{ ref('stg_trips') }}    t   ON st.trip_id  = t.trip_id
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
ORDER BY total_visits DESC