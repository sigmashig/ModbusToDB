DROP FUNCTION period_diff;
CREATE OR REPLACE FUNCTION period_diff(
    startTs TIMESTAMPTZ,
    finishTs TIMESTAMPTZ,
    fieldlist TEXT[],
    friendly_name BOOLEAN DEFAULT false,
    sort BOOLEAN DEFAULT true,
    add_unit BOOLEAN DEFAULT false
)
RETURNS TABLE (
    metricts TIMESTAMPTZ,
    metric TEXT,
    value DOUBLE PRECISION
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH fields AS (
        -- Базовий список регістрів із форматуванням імен та порядком
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
    start_values AS (
        -- Останнє значення на момент startTs (заглядаємо на 4 години назад)
        SELECT DISTINCT ON (m.register_name)
            m.register_name,
            m.value
        FROM modbus_data m
        WHERE m.register_name = ANY(fieldlist)
          AND m.timestamp <= startTs 
          AND m.timestamp >= (startTs - INTERVAL '4 hours')
        ORDER BY m.register_name, m.timestamp DESC
    ),
    finish_values AS (
        -- Останнє значення на момент finishTs (заглядаємо на 4 години назад)
        SELECT DISTINCT ON (m.register_name)
            m.register_name,
            m.value
        FROM modbus_data m
        WHERE m.register_name = ANY(fieldlist)
          AND m.timestamp <= finishTs 
          AND m.timestamp >= (finishTs - INTERVAL '4 hours')
        ORDER BY m.register_name, m.timestamp DESC
    )
    SELECT 
        finishTs AS metricts, -- Повертаємо мітку кінця періоду
        f.formatted_metric AS metric,
        (COALESCE(fv.value, 0) - COALESCE(sv.value, 0))::DOUBLE PRECISION AS value
    FROM fields f
    LEFT JOIN start_values sv ON f.reg_name = sv.register_name
    LEFT JOIN finish_values fv ON f.reg_name = fv.register_name
    ORDER BY 
        CASE WHEN sort THEN f.nr ELSE 0 END, 
        f.reg_name;
END;
$$;