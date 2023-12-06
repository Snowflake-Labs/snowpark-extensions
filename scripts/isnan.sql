
CREATE OR REPLACE  FUNCTION ISNAN(x VARIANT)
RETURNS boolean
AS 
$$
    iff(try_cast(x::varchar as float) = 'NaN', TRUE, FALSE)
$$;
