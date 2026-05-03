SELECT
    CASE route_type
        WHEN '0' THEN 'Tram / Streetcar'
        WHEN '1' THEN 'Subway / Metro'
        WHEN '2' THEN 'Rail'
        WHEN '3' THEN 'Bus'
        WHEN '4' THEN 'Ferry'
        WHEN '5' THEN 'Cable Car'
        WHEN '7' THEN 'Funicular'
        ELSE 'Unknown'
    END                             AS transit_type,
    COUNT(DISTINCT route_id)        AS total_routes,
    COUNT(DISTINCT route_short_name) AS unique_route_names
FROM {{ ref('stg_routes') }}
GROUP BY route_type
ORDER BY total_routes DESC