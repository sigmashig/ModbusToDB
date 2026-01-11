-- FUNCTION: public.parameterhistory(timestamp with time zone, timestamp with time zone)

-- DROP FUNCTION IF EXISTS public.parameterhistory(timestamp with time zone, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.parameterhistory(
	start_ts timestamp with time zone,
	end_ts timestamp with time zone DEFAULT now())
    RETURNS TABLE("time" timestamp with time zone, ainsidepower numeric, aoutsidecurrent numeric, aoutsidepower numeric, apower numeric, batchargetoday numeric, batchargetotal numeric, batdischargetoday numeric, batdischargetotal numeric, batterycurrent numeric, batteryenergy numeric, batterypower numeric, batterytemp numeric, batteryvolt numeric, binsidepower numeric, boutsidecurrent numeric, boutsidepower numeric, bpower numeric, cinsidepower numeric, coutsidecurrent numeric, coutsidepower numeric, cpower numeric, dailyused numeric, dctemp numeric, faultcode1 numeric, faultcode2 numeric, faultcode3 numeric, faultcode4 numeric, genapower numeric, genavolt numeric, genbpower numeric, genbvolt numeric, gencpower numeric, gencvolt numeric, gendailytime numeric, gentoday numeric, gentotal numeric, gentotalpower numeric, gridbuytoday numeric, gridbuytotal numeric, gridfac numeric, gridinsidetotalpac numeric, gridinsidetotalsac numeric, gridoutsidetotalpac numeric, gridoutsidetotalsac numeric, gridselltoday numeric, gridselltotal numeric, grid_vac1 numeric, grid_vac2 numeric, grid_vac3 numeric, heatsinktemp numeric, iac1 numeric, iac2 numeric, iac3 numeric, invacurrent numeric, invapower numeric, invavolt numeric, invbcurrent numeric, invbpower numeric, invbvolt numeric, invccurrent numeric, invcpower numeric, invcvolt numeric, invfac numeric, invtotalpower numeric, invtotalsac numeric, ipv1 numeric, ipv2 numeric, loadapower numeric, loadavolt numeric, loadbpower numeric, loadbvolt numeric, loadcpower numeric, loadcvolt numeric, loadtotalpac numeric, loadtotalsac numeric, pf numeric, ppv1 numeric, ppv2 numeric, pvetoday numeric, pvhistory numeric, todaypv1 numeric, todaypv2 numeric, todaypv3 numeric, todaypv4 numeric, totalused numeric, upsloadapower numeric, upsloadbpower numeric, upsloadcpower numeric, upsloadtotalpower numeric, vpv1 numeric, vpv2 numeric) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
select *
from(
SELECT 
    minuteTs as "time",
    COALESCE(ainsidepower, last_value(ainsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ainsidepower,
    COALESCE(aoutsidecurrent, last_value(aoutsidecurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS aoutsidecurrent,
    COALESCE(aoutsidepower, last_value(aoutsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS aoutsidepower,
    COALESCE(apower, last_value(apower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS apower,
    COALESCE(batchargetoday, last_value(batchargetoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batchargetoday,
    COALESCE(batchargetotal, last_value(batchargetotal) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batchargetotal,
    COALESCE(batdischargetoday, last_value(batdischargetoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batdischargetoday,
    COALESCE(batdischargetotal, last_value(batdischargetotal) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batdischargetotal,
    COALESCE(batterycurrent, last_value(batterycurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batterycurrent,
    COALESCE(batteryenergy, last_value(batteryenergy) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batteryenergy,
    COALESCE(batterypower, last_value(batterypower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batterypower,
    COALESCE(batterytemp, last_value(batterytemp) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batterytemp,
    COALESCE(batteryvolt, last_value(batteryvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS batteryvolt,
    COALESCE(binsidepower, last_value(binsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS binsidepower,
    COALESCE(boutsidecurrent, last_value(boutsidecurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS boutsidecurrent,
    COALESCE(boutsidepower, last_value(boutsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS boutsidepower,
    COALESCE(bpower, last_value(bpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS bpower,
    COALESCE(cinsidepower, last_value(cinsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS cinsidepower,
    COALESCE(coutsidecurrent, last_value(coutsidecurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS coutsidecurrent,
    COALESCE(coutsidepower, last_value(coutsidepower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS coutsidepower,
    COALESCE(cpower, last_value(cpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS cpower,
    COALESCE(dailyused, last_value(dailyused) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS dailyused,
    COALESCE(dctemp, last_value(dctemp) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS dctemp,
    COALESCE(faultcode1, last_value(faultcode1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS faultcode1,
    COALESCE(faultcode2, last_value(faultcode2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS faultcode2,
    COALESCE(faultcode3, last_value(faultcode3) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS faultcode3,
    COALESCE(faultcode4, last_value(faultcode4) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS faultcode4,
    COALESCE(genapower, last_value(genapower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS genapower,
    COALESCE(genavolt, last_value(genavolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS genavolt,
    COALESCE(genbpower, last_value(genbpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS genbpower,
    COALESCE(genbvolt, last_value(genbvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS genbvolt,
    COALESCE(gencpower, last_value(gencpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gencpower,
    COALESCE(gencvolt, last_value(gencvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gencvolt,
    COALESCE(gendailytime, last_value(gendailytime) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gendailytime,
    COALESCE(gentoday, last_value(gentoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gentoday,
    COALESCE(gentotal, last_value(gentotal) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gentotal,
    COALESCE(gentotalpower, last_value(gentotalpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gentotalpower,
    COALESCE(gridbuytoday, last_value(gridbuytoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridbuytoday,
    COALESCE(gridbuytotal, last_value(gridbuytotal) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridbuytotal,
    COALESCE(gridfac, last_value(gridfac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridfac,
    COALESCE(gridinsidetotalpac, last_value(gridinsidetotalpac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridinsidetotalpac,
    COALESCE(gridinsidetotalsac, last_value(gridinsidetotalsac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridinsidetotalsac,
    COALESCE(gridoutsidetotalpac, last_value(gridoutsidetotalpac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridoutsidetotalpac,
    COALESCE(gridoutsidetotalsac, last_value(gridoutsidetotalsac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridoutsidetotalsac,
    COALESCE(gridselltoday, last_value(gridselltoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridselltoday,
    COALESCE(gridselltotal, last_value(gridselltotal) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS gridselltotal,
    COALESCE(grid_vac1, last_value(grid_vac1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS grid_vac1,
    COALESCE(grid_vac2, last_value(grid_vac2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS grid_vac2,
    COALESCE(grid_vac3, last_value(grid_vac3) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS grid_vac3,
    COALESCE(heatsinktemp, last_value(heatsinktemp) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS heatsinktemp,
    COALESCE(iac1, last_value(iac1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS iac1,
    COALESCE(iac2, last_value(iac2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS iac2,
    COALESCE(iac3, last_value(iac3) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS iac3,
    COALESCE(invacurrent, last_value(invacurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invacurrent,
    COALESCE(invapower, last_value(invapower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invapower,
    COALESCE(invavolt, last_value(invavolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invavolt,
    COALESCE(invbcurrent, last_value(invbcurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invbcurrent,
    COALESCE(invbpower, last_value(invbpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invbpower,
    COALESCE(invbvolt, last_value(invbvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invbvolt,
    COALESCE(invccurrent, last_value(invccurrent) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invccurrent,
    COALESCE(invcpower, last_value(invcpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invcpower,
    COALESCE(invcvolt, last_value(invcvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invcvolt,
    COALESCE(invfac, last_value(invfac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invfac,
    COALESCE(invtotalpower, last_value(invtotalpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invtotalpower,
    COALESCE(invtotalsac, last_value(invtotalsac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS invtotalsac,
    COALESCE(ipv1, last_value(ipv1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ipv1,
    COALESCE(ipv2, last_value(ipv2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ipv2,
    COALESCE(loadapower, last_value(loadapower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadapower,
    COALESCE(loadavolt, last_value(loadavolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadavolt,
    COALESCE(loadbpower, last_value(loadbpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadbpower,
    COALESCE(loadbvolt, last_value(loadbvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadbvolt,
    COALESCE(loadcpower, last_value(loadcpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadcpower,
    COALESCE(loadcvolt, last_value(loadcvolt) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadcvolt,
    COALESCE(loadtotalpac, last_value(loadtotalpac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadtotalpac,
    COALESCE(loadtotalsac, last_value(loadtotalsac) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS loadtotalsac,
    COALESCE(pf, last_value(pf) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS pf,
    COALESCE(ppv1, last_value(ppv1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ppv1,
    COALESCE(ppv2, last_value(ppv2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS ppv2,
    COALESCE(pvetoday, last_value(pvetoday) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS pvetoday,
    COALESCE(pvhistory, last_value(pvhistory) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS pvhistory,
    COALESCE(todaypv1, last_value(todaypv1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS todaypv1,
    COALESCE(todaypv2, last_value(todaypv2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS todaypv2,
    COALESCE(todaypv3, last_value(todaypv3) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS todaypv3,
    COALESCE(todaypv4, last_value(todaypv4) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS todaypv4,
    COALESCE(totalused, last_value(totalused) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS totalused,
    COALESCE(upsloadapower, last_value(upsloadapower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS upsloadapower,
    COALESCE(upsloadbpower, last_value(upsloadbpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS upsloadbpower,
    COALESCE(upsloadcpower, last_value(upsloadcpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS upsloadcpower,
    COALESCE(upsloadtotalpower, last_value(upsloadtotalpower) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS upsloadtotalpower,
    COALESCE(vpv1, last_value(vpv1) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS vpv1,
    COALESCE(vpv2, last_value(vpv2) OVER (ORDER BY minuteTs ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS vpv2
FROM inverter_1m
WHERE minuteTs BETWEEN start_ts - INTERVAL '24 hours' AND end_ts
) t1
WHERE "time" BETWEEN start_ts AND end_ts
ORDER BY "time" DESC;
$BODY$;

ALTER FUNCTION public.parameterhistory(timestamp with time zone, timestamp with time zone)
    OWNER TO sigma;
