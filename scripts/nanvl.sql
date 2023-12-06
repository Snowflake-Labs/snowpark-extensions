CREATE OR REPLACE FUNCTION NANVL(x VARIANT, y VARIANT)
RETURNS VARIANT
AS
$$
    iff(try_cast(x::varchar as float) = 'NaN', y, x)
$$;