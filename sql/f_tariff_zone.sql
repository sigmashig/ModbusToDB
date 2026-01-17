CREATE OR REPLACE FUNCTION get_tariff_zone(ts timestamptz) 
RETURNS text AS $$
BEGIN
    -- Визначаємо ніч: з 23:00 до 07:00 (приклад для України)
    IF EXTRACT(HOUR FROM ts) >= 23 OR EXTRACT(HOUR FROM ts) < 7 THEN
        RETURN 'night';
    ELSE
        RETURN 'day';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;