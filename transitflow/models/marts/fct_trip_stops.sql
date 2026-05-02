SELECT
    st.trip_id,
    st.stop_id,
    st.stop_sequence,
    st.arrival_time,
    st.departure_time,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    t.route_id,
    t.trip_headsign,
    t.direction_id,
    r.route_short_name,
    r.route_long_name
FROM {{ ref('stg_stop_times') }} st
LEFT JOIN {{ ref('stg_stops') }}     s ON st.stop_id  = s.stop_id
LEFT JOIN {{ ref('stg_trips') }}     t ON st.trip_id  = t.trip_id
LEFT JOIN {{ ref('stg_routes') }}    r ON t.route_id  = r.route_id