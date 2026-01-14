--DROP FUNCTION period_values;
CREATE OR REPLACE FUNCTION period_values(
    startTs TIMESTAMPTZ,
    finishTs TIMESTAMPTZ,
    agg_period INTERVAL,
    fieldlist TEXT[],
    friendly_name BOOLEAN DEFAULT false,
    sort BOOLEAN DEFAULT true,
    add_unit BOOLEAN DEFAULT false
)
RETURNS TABLE (
    metricTs TIMESTAMPTZ,
    metric TEXT,
    value DOUBLE PRECISION
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH fields AS (
        -- Створюємо основу зі списку полів для сортування та назв
        SELECT 
            f.reg_name, 
            f.nr,
            CASE 
                WHEN NOT friendly_name THEN f.reg_name
                ELSE 
                    CASE 
                        WHEN add_unit AND p.unit IS NOT NULL AND p.unit <> '' 
                        THEN COALESCE(p.ua_friendly_name, f.reg_name) || ' (' || p.unit || ')'
                        ELSE COALESCE(p.ua_friendly_name, f.reg_name)
                    END
            END AS formatted_metric
        FROM unnest(fieldlist) WITH ORDINALITY AS f(reg_name, nr)
        LEFT JOIN params_directory p ON f.reg_name = p.register_name
    ),
    gapfilled_data AS (
        -- Використовуємо gapfill для створення сітки часу
        SELECT
            time_bucket_gapfill(agg_period, m.timestamp, startTs, finishTs) AS bucket,
            m.register_name,
            locf(
                avg(m.value),
                -- Пошук значення "назад", якщо в початковій точці немає даних
                (
                    SELECT v.value 
                    FROM modbus_data v 
                    WHERE v.register_name = m.register_name 
                      AND v.timestamp < startTs
                      AND v.timestamp >= (startTs - INTERVAL '4 hours')
                    ORDER BY v.timestamp DESC 
                    LIMIT 1
                )
            ) AS val
        FROM modbus_data m
        WHERE m.timestamp >= startTs AND m.timestamp <= finishTs
          AND m.register_name = ANY(fieldlist)
        GROUP BY 1, 2
    )
    SELECT 
        gd.bucket AS metricTs,
        f.formatted_metric AS metric,
        gd.val AS value
    FROM gapfilled_data gd
    JOIN fields f ON gd.register_name = f.reg_name
    ORDER BY 
        gd.bucket, 
        CASE WHEN sort THEN f.nr ELSE 0 END;
END;
$$;