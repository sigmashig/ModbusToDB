CREATE MATERIALIZED VIEW inverter_tariff_stats
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', "timestamp") AS day,
    device_id,
    register_name,
    get_tariff_zone("timestamp") AS zone,
    
    (last(value, "timestamp") - first(value, "timestamp")) as diff_value,
    last(value, "timestamp") as end_value,
    MAX(value) as max_value,
    MIN(value) as min_value
FROM modbus_data
WHERE register_name IN (
    'batChargeTotal(kWh)','batDischargeTotal(kWh)', 'dailyUsed(kWh)','genTotal(kWh)',
	'gridBuyTotal(kWh)','gridSellTotal(kWh)','pvHistory(kWh)','todayPv1(kWh)','todayPv2(kWh)','totalUsed(kWh)'
)
GROUP BY day, device_id, register_name, zone
WITH NO DATA;

--- 2. ПОЛІТИКА ОНОВЛЕННЯ
--- Перевіряємо останні 14 днів на предмет нових даних ("заднім числом") щогодини.
SELECT add_continuous_aggregate_policy('inverter_tariff_stats',
    start_offset => INTERVAL '14 days',
    end_offset => INTERVAL '0 hours',
    schedule_interval => INTERVAL '1 hour');

--- 3. СТИСНЕННЯ (COMPRESSION)
--- Вмикаємо стовпчикове стиснення для агрегату.
ALTER MATERIALIZED VIEW inverter_tariff_stats SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'device_id'
);

--- Стискаємо дані, які старші за 30 днів.
SELECT add_compression_policy('inverter_tariff_stats', INTERVAL '30 days');