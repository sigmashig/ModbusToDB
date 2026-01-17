CREATE OR REPLACE VIEW daily_tariff_report AS
SELECT 
    day,
    device_id,
    register_name,
    -- Значення для дня
    MAX(diff_value) FILTER (WHERE zone = 'day') as day_diff,
    -- Значення для ночі
    MAX(diff_value) FILTER (WHERE zone = 'night') as night_diff,
    -- Загальне за добу
    SUM(diff_value) as total_diff
FROM inverter_tariff_stats
GROUP BY day, device_id, register_name;