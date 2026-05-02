SELECT
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    loaded_at
FROM raw.routes
WHERE route_id IS NOT NULL