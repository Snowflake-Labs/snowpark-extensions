-- array_join(array, delimiter[, nullReplacement]) 
-- Concatenates the elements of the given array using the delimiter 
-- and an optional string to replace nulls. 
-- If no value is set for nullReplacement, any null value is filtered.


CREATE OR REPLACE FUNCTION PUBLIC.ARRAY_JOIN(ARRAY1 ARRAY, SEP VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
   array_to_string(array_compact(ARRAY1), SEP)
$$;

CREATE OR REPLACE FUNCTION PUBLIC.ARRAY_JOIN(ARRAY1 ARRAY, SEP VARCHAR, VAL_FOR_NULL VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
   array_to_string(ARRAY_NULL_REPLACE(ARRAY1,VAL_FOR_NULL), SEP)
$$;
