DROP MATERIALIZED VIEW inverter_day;
CREATE MATERIALIZED VIEW inverter_day
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', "timestamp") AS "metricTs",
    device_id,

    -- aInsidePower(W)
    MIN(value) FILTER (WHERE register_name = 'aInsidePower(W)' AND value >= 100) as n_aInsidePower,
    MAX(value) FILTER (WHERE register_name = 'aInsidePower(W)') as x_aInsidePower,
    AVG(value) FILTER (WHERE register_name = 'aInsidePower(W)' AND value >= 100) as a_aInsidePower,

    -- aOutsidePower(W)
    MIN(value) FILTER (WHERE register_name = 'aOutsidePower(W)' AND value >= 100) as n_aOutsidePower,
    MAX(value) FILTER (WHERE register_name = 'aOutsidePower(W)') as x_aOutsidePower,
    AVG(value) FILTER (WHERE register_name = 'aOutsidePower(W)' AND value >= 100) as a_aOutsidePower,


-- batChargeTotal(kWh)
    (
      last(value, "timestamp") FILTER (WHERE register_name = 'batChargeTotal(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'batChargeTotal(kWh)')
    ) as d_batChargeTotal,

    -- e_batChargeTotal (просто останнє значення за добу)
    last(value, "timestamp") FILTER (WHERE register_name = 'batChargeTotal(kWh)') as e_batChargeTotal,

    -- batDischargeTotal(kWh)
    (
      last(value, "timestamp") FILTER (WHERE register_name = 'batDischargeTotal(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'batDischargeTotal(kWh)')
    ) as d_batDischargeTotal,
    last(value, "timestamp") FILTER (WHERE register_name = 'batDischargeTotal(kWh)') as e_batDischargeTotal,

    -- batteryEnergy(%)
    MIN(value) FILTER (WHERE register_name = 'batteryEnergy(%)') as n_batteryEnergy,
    MAX(value) FILTER (WHERE register_name = 'batteryEnergy(%)') as x_batteryEnergy,
    AVG(value) FILTER (WHERE register_name = 'batteryEnergy(%)') as a_batteryEnergy,

    -- batteryPower(W)
    MIN(value) FILTER (WHERE register_name = 'batteryPower(W)' AND value >= 100) as n_batteryPower,
    MAX(value) FILTER (WHERE register_name = 'batteryPower(W)') as x_batteryPower,
    AVG(value) FILTER (WHERE register_name = 'batteryPower(W)' AND value >= 100) as a_batteryPower,

    -- batteryTemp(C)
    MIN(value) FILTER (WHERE register_name = 'batteryTemp(C)') as n_batteryTemp,
    MAX(value) FILTER (WHERE register_name = 'batteryTemp(C)') as x_batteryTemp,
    AVG(value) FILTER (WHERE register_name = 'batteryTemp(C)') as a_batteryTemp,

    -- batteryVolt(V)
    MIN(value) FILTER (WHERE register_name = 'batteryVolt(V)' AND value >= 100) as n_batteryVolt,
    MAX(value) FILTER (WHERE register_name = 'batteryVolt(V)') as x_batteryVolt,
    AVG(value) FILTER (WHERE register_name = 'batteryVolt(V)' AND value >= 100) as a_batteryVolt,

    -- bInsidePower(W)
    MIN(value) FILTER (WHERE register_name = 'bInsidePower(W)' AND value >= 100) as n_bInsidePower,
    MAX(value) FILTER (WHERE register_name = 'bInsidePower(W)') as x_bInsidePower,
    AVG(value) FILTER (WHERE register_name = 'bInsidePower(W)' AND value >= 100) as a_bInsidePower,

    -- bOutsidePower(W)
    MIN(value) FILTER (WHERE register_name = 'bOutsidePower(W)' AND value >= 100) as n_bOutsidePower,
    MAX(value) FILTER (WHERE register_name = 'bOutsidePower(W)') as x_bOutsidePower,
    AVG(value) FILTER (WHERE register_name = 'bOutsidePower(W)' AND value >= 100) as a_bOutsidePower,

    -- cInsidePower(W)
    MIN(value) FILTER (WHERE register_name = 'cInsidePower(W)' AND value >= 100) as n_cInsidePower,
    MAX(value) FILTER (WHERE register_name = 'cInsidePower(W)') as x_cInsidePower,
    AVG(value) FILTER (WHERE register_name = 'cInsidePower(W)' AND value >= 100) as a_cInsidePower,

    -- dailyUsed(kWh)
	    (
      last(value, "timestamp") FILTER (WHERE register_name = 'dailyUsed(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'dailyUsed(kWh)')
    ) as d_dailyUsed,
    last(value, "timestamp") FILTER (WHERE register_name = 'dailyUsed(kWh)') as e_dailyUsed,

    -- dcTemp(C)
    MIN(value) FILTER (WHERE register_name = 'dcTemp(C)') as n_dcTemp,
    MAX(value) FILTER (WHERE register_name = 'dcTemp(C)') as x_dcTemp,
    AVG(value) FILTER (WHERE register_name = 'dcTemp(C)') as a_dcTemp,

    -- heatSinkTemp(C)
    MIN(value) FILTER (WHERE register_name = 'heatSinkTemp(C)') as n_heatSinkTemp,
    MAX(value) FILTER (WHERE register_name = 'heatSinkTemp(C)') as x_heatSinkTemp,
    AVG(value) FILTER (WHERE register_name = 'heatSinkTemp(C)') as a_heatSinkTemp,


    -- FaultCodes (CNT)
    COUNT(value) FILTER (WHERE register_name = 'FaultCode1(NA)') as c_FaultCode1,
    COUNT(value) FILTER (WHERE register_name = 'FaultCode2(NA)') as c_FaultCode2,
    COUNT(value) FILTER (WHERE register_name = 'FaultCode3(NA)') as c_FaultCode3,
    COUNT(value) FILTER (WHERE register_name = 'FaultCode4(NA)') as c_FaultCode4,

    -- genAPower / Volt
    MIN(value) FILTER (WHERE register_name = 'genAPower(W)' AND value >= 100) as n_genAPower,
    MAX(value) FILTER (WHERE register_name = 'genAPower(W)') as x_genAPower,
    AVG(value) FILTER (WHERE register_name = 'genAPower(W)' AND value >= 100) as a_genAPower,

    MIN(value) FILTER (WHERE register_name = 'genAVolt(V)' AND value >= 100) as n_genAVolt,
    MAX(value) FILTER (WHERE register_name = 'genAVolt(V)') as x_genAVolt,
    AVG(value) FILTER (WHERE register_name = 'genAVolt(V)' AND value >= 100) as a_genAVolt,

    -- genBPower / Volt
    MIN(value) FILTER (WHERE register_name = 'genBPower(W)' AND value >= 100) as n_genBPower,
    MAX(value) FILTER (WHERE register_name = 'genBPower(W)') as x_genBPower,
    AVG(value) FILTER (WHERE register_name = 'genBPower(W)' AND value >= 100) as a_genBPower,

    MIN(value) FILTER (WHERE register_name = 'genBVolt(V)' AND value >= 100) as n_genBVolt,
    MAX(value) FILTER (WHERE register_name = 'genBVolt(V)') as x_genBVolt,
    AVG(value) FILTER (WHERE register_name = 'genBVolt(V)' AND value >= 100) as a_genBVolt,

    -- genCPower / Volt
    MIN(value) FILTER (WHERE register_name = 'genCPower(W)' AND value >= 100) as n_genCPower,
    MAX(value) FILTER (WHERE register_name = 'genCPower(W)') as x_genCPower,
    AVG(value) FILTER (WHERE register_name = 'genCPower(W)' AND value >= 100) as a_genCPower,

    MIN(value) FILTER (WHERE register_name = 'genCVolt(V)' AND value >= 100) as n_genCVolt,
    MAX(value) FILTER (WHERE register_name = 'genCVolt(V)') as x_genCVolt,
    AVG(value) FILTER (WHERE register_name = 'genCVolt(V)' AND value >= 100) as a_genCVolt,


    -- genTotal / genDailyTime
	(
      last(value, "timestamp") FILTER (WHERE register_name = 'genTotal(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'genTotal(kWh)')
    ) as d_genTotal,
	last(value, "timestamp") FILTER (WHERE register_name = 'genTotal(kWh)') as e_genTotal,

	(
      last(value, "timestamp") FILTER (WHERE register_name = 'genDailyTime(h)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'genDailyTime(h)')
    ) as d_genDailyTime,

    -- genTotalPower(W)
    MIN(value) FILTER (WHERE register_name = 'genTotalPower(W)' AND value >= 100) as n_genTotalPower,
    MAX(value) FILTER (WHERE register_name = 'genTotalPower(W)') as x_genTotalPower,
    AVG(value) FILTER (WHERE register_name = 'genTotalPower(W)' AND value >= 100) as a_genTotalPower,

    -- gridInsideTotalPac
    MIN(value) FILTER (WHERE register_name = 'gridInsideTotalPac(W)' AND value >= 100) as n_gridInsideTotalPac,
    MAX(value) FILTER (WHERE register_name = 'gridInsideTotalPac(W)') as x_gridInsideTotalPac,
    AVG(value) FILTER (WHERE register_name = 'gridInsideTotalPac(W)' AND value >= 100) as a_gridInsideTotalPac,

    -- gridOutsideTotalPac
    MIN(value) FILTER (WHERE register_name = 'gridOutsideTotalPac(W)' AND value >= 100) as n_gridOutsideTotalPac,
    MAX(value) FILTER (WHERE register_name = 'gridOutsideTotalPac(W)') as x_gridOutsideTotalPac,
    AVG(value) FILTER (WHERE register_name = 'gridOutsideTotalPac(W)' AND value >= 100) as a_gridOutsideTotalPac,

    -- gridInsideTotalSac
    MIN(value) FILTER (WHERE register_name = 'gridInsideTotalSac(VA)' AND value >= 100) as n_gridInsideTotalSac,
    MAX(value) FILTER (WHERE register_name = 'gridInsideTotalSac(VA)') as x_gridInsideTotalSac,
    AVG(value) FILTER (WHERE register_name = 'gridInsideTotalSac(VA)' AND value >= 100) as a_gridInsideTotalSac,

    -- gridOutsideTotalSac
    MIN(value) FILTER (WHERE register_name = 'gridOutsideTotalSac(VA)' AND value >= 100) as n_gridOutsideTotalSac,
    MAX(value) FILTER (WHERE register_name = 'gridOutsideTotalSac(VA)') as x_gridOutsideTotalSac,
    AVG(value) FILTER (WHERE register_name = 'gridOutsideTotalSac(VA)' AND value >= 100) as a_gridOutsideTotalSac,

	-- Grid vac1(V)
    MIN(value) FILTER (WHERE register_name = 'Grid vac1(V)' AND value >= 100) as n_gridVac1,
    MAX(value) FILTER (WHERE register_name = 'Grid vac1(V)') as x_gridVac1,
    AVG(value) FILTER (WHERE register_name = 'Grid vac1(V)' AND value >= 100) as a_gridVac1,

	-- Grid vac2(V)
    MIN(value) FILTER (WHERE register_name = 'Grid vac2(V)' AND value >= 100) as n_gridVac2,
    MAX(value) FILTER (WHERE register_name = 'Grid vac2(V)') as x_gridVac2,
    AVG(value) FILTER (WHERE register_name = 'Grid vac2(V)' AND value >= 100) as a_gridVac2,

	-- Grid vac3(V)
    MIN(value) FILTER (WHERE register_name = 'Grid vac3(V)' AND value >= 100) as n_gridVac3,
    MAX(value) FILTER (WHERE register_name = 'Grid vac3(V)') as x_gridVac3,
    AVG(value) FILTER (WHERE register_name = 'Grid vac3(V)' AND value >= 100) as a_gridVac3,


    -- Grid Buy / Sell
    (
      last(value, "timestamp") FILTER (WHERE register_name = 'gridBuyTotal(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'gridBuyTotal(kWh)')
    ) as d_gridBuyTotal,
    last(value, "timestamp") FILTER (WHERE register_name = 'gridBuyTotal(kWh)') as e_gridBuyTotal,
    (
      last(value, "timestamp") FILTER (WHERE register_name = 'gridSellTotal(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'gridSellTotal(kWh)')
    ) as d_gridSellTotal,
    last(value, "timestamp") FILTER (WHERE register_name = 'gridSellTotal(kWh)') as e_gridSellTotal,

    -- gridFac
    MIN(value) FILTER (WHERE register_name = 'gridFac(Hz)') as n_gridFac,
    MAX(value) FILTER (WHERE register_name = 'gridFac(Hz)') as x_gridFac,
    AVG(value) FILTER (WHERE register_name = 'gridFac(Hz)') as a_gridFac,

    -- invFac
    MIN(value) FILTER (WHERE register_name = 'invFac(Hz)') as n_invFac,
    MAX(value) FILTER (WHERE register_name = 'invFac(Hz)') as x_invFac,
    AVG(value) FILTER (WHERE register_name = 'invFac(Hz)') as a_invFac,


    -- invAPower / invAVolt
    MIN(value) FILTER (WHERE register_name = 'invAPower(W)' AND value >= 100) as n_invAPower,
    MAX(value) FILTER (WHERE register_name = 'invAPower(W)') as x_invAPower,
    AVG(value) FILTER (WHERE register_name = 'invAPower(W)' AND value >= 100) as a_invAPower,

    MIN(value) FILTER (WHERE register_name = 'invAVolt(V)' AND value >= 100) as n_invAVolt,
    MAX(value) FILTER (WHERE register_name = 'invAVolt(V)') as x_invAVolt,
    AVG(value) FILTER (WHERE register_name = 'invAVolt(V)' AND value >= 100) as a_invAVolt,

    -- invBPower / invBVolt
    MIN(value) FILTER (WHERE register_name = 'invBPower(W)' AND value >= 100) as n_invBPower,
    MAX(value) FILTER (WHERE register_name = 'invBPower(W)') as x_invBPower,
    AVG(value) FILTER (WHERE register_name = 'invBPower(W)' AND value >= 100) as a_invBPower,

    MIN(value) FILTER (WHERE register_name = 'invBVolt(V)' AND value >= 100) as n_invBVolt,
    MAX(value) FILTER (WHERE register_name = 'invBVolt(V)') as x_invBVolt,
    AVG(value) FILTER (WHERE register_name = 'invBVolt(V)' AND value >= 100) as a_invBVolt,

    -- invCPower / invCVolt
    MIN(value) FILTER (WHERE register_name = 'invCPower(W)' AND value >= 100) as n_invCPower,
    MAX(value) FILTER (WHERE register_name = 'invCPower(W)') as x_invCPower,
    AVG(value) FILTER (WHERE register_name = 'invCPower(W)' AND value >= 100) as a_invCPower,

    MIN(value) FILTER (WHERE register_name = 'invCVolt(V)' AND value >= 100) as n_invCVolt,
    MAX(value) FILTER (WHERE register_name = 'invCVolt(V)') as x_invCVolt,
    AVG(value) FILTER (WHERE register_name = 'invCVolt(V)' AND value >= 100) as a_invCVolt,

	-- invTotalPower(W)
    MIN(value) FILTER (WHERE register_name = 'invTotalPower(W)' AND value >= 100) as n_invTotalPower,
    MAX(value) FILTER (WHERE register_name = 'invTotalPower(W)') as x_invTotalPower,
    AVG(value) FILTER (WHERE register_name = 'invTotalPower(W)' AND value >= 100) as a_invTotalPower,

	-- invTotalSac(W)
    MIN(value) FILTER (WHERE register_name = 'invTotalSac(VA)' AND value >= 100) as n_invTotalSac,
    MAX(value) FILTER (WHERE register_name = 'invTotalSac(VA)') as x_invTotalSac,
    AVG(value) FILTER (WHERE register_name = 'invTotalSac(VA)' AND value >= 100) as a_invTotalSac,

    -- loadAPower / loadAVolt
    MIN(value) FILTER (WHERE register_name = 'loadAPower(W)' AND value >= 100) as n_loadAPower,
    MAX(value) FILTER (WHERE register_name = 'loadAPower(W)') as x_loadAPower,
    AVG(value) FILTER (WHERE register_name = 'loadAPower(W)' AND value >= 100) as a_loadAPower,

    MIN(value) FILTER (WHERE register_name = 'loadAVolt(V)' AND value >= 100) as n_loadAVolt,
    MAX(value) FILTER (WHERE register_name = 'loadAVolt(V)') as x_loadAVolt,
    AVG(value) FILTER (WHERE register_name = 'loadAVolt(V)' AND value >= 100) as a_loadAVolt,

    -- loadAPower / loadBVolt
    MIN(value) FILTER (WHERE register_name = 'loadBPower(W)' AND value >= 100) as n_loadBPower,
    MAX(value) FILTER (WHERE register_name = 'loadBPower(W)') as x_loadBPower,
    AVG(value) FILTER (WHERE register_name = 'loadBPower(W)' AND value >= 100) as a_loadBPower,

    MIN(value) FILTER (WHERE register_name = 'loadBVolt(V)' AND value >= 100) as n_loadBVolt,
    MAX(value) FILTER (WHERE register_name = 'loadBVolt(V)') as x_loadBVolt,
    AVG(value) FILTER (WHERE register_name = 'loadBVolt(V)' AND value >= 100) as a_loadBVolt,

    -- loadCPower / loadCVolt
    MIN(value) FILTER (WHERE register_name = 'loadCPower(W)' AND value >= 100) as n_loadCPower,
    MAX(value) FILTER (WHERE register_name = 'loadCPower(W)') as x_loadCPower,
    AVG(value) FILTER (WHERE register_name = 'loadCPower(W)' AND value >= 100) as a_loadCPower,

    MIN(value) FILTER (WHERE register_name = 'loadCVolt(V)' AND value >= 100) as n_loadCVolt,
    MAX(value) FILTER (WHERE register_name = 'loadCVolt(V)') as x_loadCVolt,
    AVG(value) FILTER (WHERE register_name = 'loadCVolt(V)' AND value >= 100) as a_loadCVolt,


    -- ipv / vpv/ppv (PV strings)
    MIN(value) FILTER (WHERE register_name = 'ipv1(A)') as n_ipv1,
    MAX(value) FILTER (WHERE register_name = 'ipv1(A)') as x_ipv1,
    MIN(value) FILTER (WHERE register_name = 'vpv1(V)' AND value >= 100) as n_vpv1,
    AVG(value) FILTER (WHERE register_name = 'vpv1(V)' AND value >= 100) as a_vpv1,
    MIN(value) FILTER (WHERE register_name = 'ppv1(W)' AND value >= 100) as n_ppv1,
    MAX(value) FILTER (WHERE register_name = 'ppv1(W)') as x_ppv1,
    AVG(value) FILTER (WHERE register_name = 'ppv1(W)' AND value >= 100) as a_ppv1,

    MIN(value) FILTER (WHERE register_name = 'ipv2(A)') as n_ipv2,
    MAX(value) FILTER (WHERE register_name = 'ipv2(A)') as x_ipv2,
    MIN(value) FILTER (WHERE register_name = 'vpv2(V)' AND value >= 100) as n_vpv2,
    AVG(value) FILTER (WHERE register_name = 'vpv2(V)' AND value >= 100) as a_vpv2,
    MIN(value) FILTER (WHERE register_name = 'ppv2(W)' AND value >= 100) as n_ppv2,
    MAX(value) FILTER (WHERE register_name = 'ppv2(W)') as x_ppv2,
    AVG(value) FILTER (WHERE register_name = 'ppv2(W)' AND value >= 100) as a_ppv2,


    (
      last(value, "timestamp") FILTER (WHERE register_name = 'pvHistory(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'pvHistory(kWh)')
    ) as d_pvHistory,
    last(value, "timestamp") FILTER (WHERE register_name = 'pvHistory(kWh)') as e_pvHistory,

    (
      last(value, "timestamp") FILTER (WHERE register_name = 'todayPv1(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'todayPv1(kWh)')
    ) as d_todayPv1,
    last(value, "timestamp") FILTER (WHERE register_name = 'todayPv1(kWh)') as e_todayPv1,

    (
      last(value, "timestamp") FILTER (WHERE register_name = 'todayPv2(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'todayPv2(kWh)')
    ) as d_todayPv2,
    last(value, "timestamp") FILTER (WHERE register_name = 'todayPv2(kWh)') as e_todayPv2,


    -- loadTotalPac
    MIN(value) FILTER (WHERE register_name = 'loadTotalPac(W)' AND value >= 100) as n_loadTotalPac,
    MAX(value) FILTER (WHERE register_name = 'loadTotalPac(W)') as x_loadTotalPac,
    AVG(value) FILTER (WHERE register_name = 'loadTotalPac(W)' AND value >= 100) as a_loadTotalPac,

    -- loadTotalSac
    MIN(value) FILTER (WHERE register_name = 'loadTotalSac(VA)' AND value >= 100) as n_loadTotalSac,
    MAX(value) FILTER (WHERE register_name = 'loadTotalSac(VA)') as x_loadTotalSac,
    AVG(value) FILTER (WHERE register_name = 'loadTotalSac(VA)' AND value >= 100) as a_loadTotalSac,

    -- pf(NA)
    MIN(value) FILTER (WHERE register_name = 'pf(NA)') as n_pf,
    MAX(value) FILTER (WHERE register_name = 'pf(NA)') as x_pf,
    AVG(value) FILTER (WHERE register_name = 'pf(NA)') as a_pf,



    -- totalUsed(kWh)
    (
      last(value, "timestamp") FILTER (WHERE register_name = 'totalUsed(kWh)') - 
      first(value, "timestamp") FILTER (WHERE register_name = 'totalUsed(kWh)')
    ) as d_totalUsed,
    last(value, "timestamp") FILTER (WHERE register_name = 'totalUsed(kWh)') as e_totalUsed

FROM modbus_data
GROUP BY 1, 2
WITH NO DATA;

--- 2. ПОЛІТИКА ОНОВЛЕННЯ
--- Перевіряємо останні 14 днів на предмет нових даних ("заднім числом") щогодини.
SELECT add_continuous_aggregate_policy('inverter_day',
    start_offset => INTERVAL '14 days',
    end_offset => INTERVAL '0 hours',
    schedule_interval => INTERVAL '1 hour');

--- 3. СТИСНЕННЯ (COMPRESSION)
--- Вмикаємо стовпчикове стиснення для агрегату.
ALTER MATERIALIZED VIEW inverter_day SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'device_id'
);

--- Стискаємо дані, які старші за 30 днів.
SELECT add_compression_policy('inverter_day', INTERVAL '30 days');