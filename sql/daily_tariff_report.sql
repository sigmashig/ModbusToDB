DROP VIEW daily_tariff_report;
CREATE OR REPLACE VIEW daily_tariff_report AS
WITH stats AS (
    SELECT  
        day::date AS day,
        device_id,
        register_name,
        MAX(diff_value) FILTER (WHERE zone = 'day')   AS day_diff,
        MAX(diff_value) FILTER (WHERE zone = 'night') AS night_diff,
        MAX(diff_value) AS total_diff
    FROM inverter_tariff_stats
    GROUP BY day::date, device_id, register_name
)
SELECT 
    s.day,
    s.device_id,
    s.register_name,
    s.day_diff,
    s.day_diff * p.d_price   AS day_cost,
    s.night_diff,
    s.night_diff * p.n_price AS night_cost,
    s.total_diff
FROM stats s
LEFT JOIN get_eprice(day) p ON TRUE;