-- FUNCTION: public.last_value_at(timestamp with time zone)

-- DROP FUNCTION IF EXISTS public.last_value_at(timestamp with time zone);

CREATE OR REPLACE FUNCTION public.last_value_at(
	target_time timestamp with time zone DEFAULT now())
    RETURNS TABLE("timestamp" timestamp with time zone, device_id integer, register_name text, value double precision) 
    LANGUAGE 'sql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
    SELECT "timestamp",
           device_id,
           register_name,
           value
    FROM (
        SELECT m.device_id,
               m."timestamp",
               m.register_name,
               m.value,
               row_number() OVER (
                   PARTITION BY m.register_name, m.device_id 
                   ORDER BY m."timestamp" DESC
               ) AS rn
        FROM public.modbus_data m
        WHERE m."timestamp" <= target_time
          -- Оптимізація для TimescaleDB: шукаємо не далі ніж за 3 дні від вказаної дати
          AND m."timestamp" >= target_time - INTERVAL '3 days'
    ) t
    WHERE rn = 1;
$BODY$;

ALTER FUNCTION public.last_value_at(timestamp with time zone)
    OWNER TO sigma;
