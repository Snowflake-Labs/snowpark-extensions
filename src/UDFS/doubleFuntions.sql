
CREATE OR REPLACE  FUNCTION ISNAN(x VARIANT)
RETURNS boolean
AS 
$$
    iff(try_cast(x::varchar as float) = 'NaN', TRUE, FALSE)
$$;

CREATE OR REPLACE FUNCTION NANVL(x VARIANT, y VARIANT)
RETURNS VARIANT
AS
$$
    iff(try_cast(x::varchar as float) = 'NaN', y, x)
$$;
CREATE OR REPLACE FUNCTION CONV(VALUE_IN STRING, OLD_BASE FLOAT, NEW_BASE FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    return parseInt(VALUE_IN, Math.floor(OLD_BASE)).toString(Math.floor(NEW_BASE)).toUpperCase();
$$;