
DROP MATERIALIZED VIEW inverter_1m CASCADE;
CREATE MATERIALIZED VIEW inverter_1m
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', "timestamp") AS minuteTs,
    last(value, "timestamp") FILTER (WHERE (register_name = 'aInsidePower(W)')) AS ainsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'aOutsideCurrent(A)')) AS aoutsidecurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'aOutsidePower(W)')) AS aoutsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'aPower(W)')) AS apower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batChargeToday(kWh)')) AS batchargetoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batChargeTotal(kWh)')) AS batchargetotal,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batDischargeToday(kWh)')) AS batdischargetoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batDischargeTotal(kWh)')) AS batdischargetotal,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batteryCurrent(A)')) AS batterycurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batteryEnergy(%)')) AS batteryenergy,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batteryPower(W)')) AS batterypower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batteryTemp(C)')) AS batterytemp,
    last(value, "timestamp") FILTER (WHERE (register_name = 'batteryVolt(V)')) AS batteryvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'bInsidePower(W)')) AS binsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'bOutsideCurrent(A)')) AS boutsidecurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'bOutsidePower(W)')) AS boutsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'bPower(W)')) AS bpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'cInsidePower(W)')) AS cinsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'cOutsideCurrent(A)')) AS coutsidecurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'cOutsidePower(W)')) AS coutsidepower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'cPower(W)')) AS cpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'dailyUsed(kWh)')) AS dailyused,
    last(value, "timestamp") FILTER (WHERE (register_name = 'dcTemp(C)')) AS dctemp,
    last(value, "timestamp") FILTER (WHERE (register_name = 'FaultCode1(NA)')) AS faultcode1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'FaultCode2(NA)')) AS faultcode2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'FaultCode3(NA)')) AS faultcode3,
    last(value, "timestamp") FILTER (WHERE (register_name = 'FaultCode4(NA)')) AS faultcode4,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genAPower(W)')) AS genapower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genAVolt(V)')) AS genavolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genBPower(W)')) AS genbpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genBVolt(V)')) AS genbvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genCPower(W)')) AS gencpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genCVolt(V)')) AS gencvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genDailyTime(h)')) AS gendailytime,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genToday(kWh)')) AS gentoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genTotal(kWh)')) AS gentotal,
    last(value, "timestamp") FILTER (WHERE (register_name = 'genTotalPower(W)')) AS gentotalpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridBuyToday(kWh)')) AS gridbuytoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridBuyTotal(kWh)')) AS gridbuytotal,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridFac(Hz)')) AS gridfac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridInsideTotalPac(W)')) AS gridinsidetotalpac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridInsideTotalSac(VA)')) AS gridinsidetotalsac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridOutsideTotalPac(W)')) AS gridoutsidetotalpac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridOutsideTotalSac(VA)')) AS gridoutsidetotalsac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridSellToday(kWh)')) AS gridselltoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'gridSellTotal(kWh)')) AS gridselltotal,
    last(value, "timestamp") FILTER (WHERE (register_name = 'Grid vac1(V)')) AS grid_vac1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'Grid vac2(V)')) AS grid_vac2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'Grid vac3(V)')) AS grid_vac3,
    last(value, "timestamp") FILTER (WHERE (register_name = 'heatsinkTemp(C)')) AS heatsinktemp,
    last(value, "timestamp") FILTER (WHERE (register_name = 'iac1(A)')) AS iac1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'iac2(A)')) AS iac2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'iac3(A)')) AS iac3,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invACurrent(A)')) AS invacurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invAPower(W)')) AS invapower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invAVolt(V)')) AS invavolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invBCurrent(A)')) AS invbcurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invBPower(W)')) AS invbpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invBVolt(V)')) AS invbvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invCCurrent(A)')) AS invccurrent,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invCPower(W)')) AS invcpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invCVolt(V)')) AS invcvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invFac(Hz)')) AS invfac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invTotalPower(W)')) AS invtotalpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'invTotalSac(VA)')) AS invtotalsac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'ipv1(A)')) AS ipv1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'ipv2(A)')) AS ipv2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadAPower(W)')) AS loadapower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadAVolt(V)')) AS loadavolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadBPower(W)')) AS loadbpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadBVolt(V)')) AS loadbvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadCPower(W)')) AS loadcpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadCVolt(V)')) AS loadcvolt,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadTotalPac(W)')) AS loadtotalpac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'loadTotalSac(VA)')) AS loadtotalsac,
    last(value, "timestamp") FILTER (WHERE (register_name = 'PF(NA)')) AS pf,
    last(value, "timestamp") FILTER (WHERE (register_name = 'ppv1(W)')) AS ppv1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'ppv2(W)')) AS ppv2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'pvEToday(kWh)')) AS pvetoday,
    last(value, "timestamp") FILTER (WHERE (register_name = 'pvHistory(kWh)')) AS pvhistory,
    last(value, "timestamp") FILTER (WHERE (register_name = 'todayPv1(kWh)')) AS todaypv1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'todayPv2(kWh)')) AS todaypv2,
    last(value, "timestamp") FILTER (WHERE (register_name = 'todayPv3(kWh)')) AS todaypv3,
    last(value, "timestamp") FILTER (WHERE (register_name = 'todayPv4(kWh)')) AS todaypv4,
    last(value, "timestamp") FILTER (WHERE (register_name = 'totalUsed(kWh)')) AS totalused,
    last(value, "timestamp") FILTER (WHERE (register_name = 'upsLoadAPower(W)')) AS upsloadapower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'upsLoadBPower(W)')) AS upsloadbpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'upsLoadCPower(W)')) AS upsloadcpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'upsLoadTotalPower(W)')) AS upsloadtotalpower,
    last(value, "timestamp") FILTER (WHERE (register_name = 'vpv1(V)')) AS vpv1,
    last(value, "timestamp") FILTER (WHERE (register_name = 'vpv2(V)')) AS vpv2
FROM modbus_data
GROUP BY 1
WITH NO DATA;

SELECT add_continuous_aggregate_policy('inverter_1m',
    start_offset => INTERVAL '2 hours', -- Як далеко в минуле перевіряти зміни
    end_offset   => INTERVAL '1 minute', -- Не агрегувати останні дані, щоб уникнути конфліктів запису
    schedule_interval => INTERVAL '1 minute'); -- Як часто запускати оновлення

ALTER MATERIALIZED VIEW inverter_1m 
SET (
  timescaledb.compress = true,
  -- TimescaleDB автоматично додасть minutets в compress_orderby.
  timescaledb.compress_segmentby = '' 
);

SELECT add_compression_policy('inverter_1m', 
  compress_after => INTERVAL '7 days');
  
CALL refresh_continuous_aggregate('inverter_1m', '2024-01-01', '2024-01-10');
