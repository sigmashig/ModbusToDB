--DROP FUNCTION last_value_at;
CREATE OR REPLACE FUNCTION last_value_at(
    ts TIMESTAMPTZ,
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
        -- 1. Створюємо основу запиту з вашого масиву (гарантує наявність усіх рядків)
        SELECT unnest_field AS reg_name, nr
        FROM unnest(fieldlist) WITH ORDINALITY AS t(unnest_field, nr)
    ),
    latest_data AS (
        -- 2. Шукаємо останні значення в межах 4-годинного вікна
        -- DISTINCT ON забезпечує вибір тільки найсвіжішого запису для кожного імені
        SELECT DISTINCT ON (m.register_name)
            m.register_name,
            m.value
        FROM modbus_data m
        WHERE m.register_name = ANY(fieldlist)
          AND m.timestamp <= ts 
          AND m.timestamp >= (ts - INTERVAL '4 hours')
        ORDER BY m.register_name, m.timestamp DESC
    )
    -- 3. З'єднуємо список полів з отриманими даними та довідником
    SELECT 
        ts AS metricTs,
        CASE 
            WHEN NOT friendly_name THEN f.reg_name
            ELSE 
                CASE 
                    WHEN add_unit AND p.unit IS NOT NULL AND p.unit <> '' 
                    THEN COALESCE(p.ua_friendly_name, f.reg_name) || ' (' || p.unit || ')'
                    ELSE COALESCE(p.ua_friendly_name, f.reg_name)
                END
        END AS metric,
        ld.value -- Буде NULL, якщо даних за 4 години не знайдено
    FROM fields f
    LEFT JOIN latest_data ld ON f.reg_name = ld.register_name
    LEFT JOIN params_directory p ON f.reg_name = p.register_name
    ORDER BY 
        CASE WHEN sort THEN f.nr ELSE 0 END, 
        f.reg_name;
END;
$$;