DROP FUNCTION get_eprice;
CREATE OR REPLACE FUNCTION get_eprice(d0 timestamp)
RETURNS TABLE(d_price double precision, n_price double precision) AS $$
BEGIN
    RETURN QUERY
    SELECT dprice as d_price, nprice as n_price
    FROM "ePrice"
    WHERE dt <= d0
    ORDER BY dt DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;