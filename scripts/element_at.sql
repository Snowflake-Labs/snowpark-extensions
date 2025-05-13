-- Returns the element at the given index from an array.
-- Supports 1-based indexing (like Spark) and negative indices for reverse access.
-- Example: element_at(ARRAY_CONSTRUCT('a','b','c'), -1) returns 'c'
-- NOTE: In snowflake we dont return INDEX errors

-- Returns the value associated with the given key in an object (map).
-- Equivalent to accessing a value by key in a dictionary.
-- Example: element_at(OBJECT_CONSTRUCT('a', 1.0), 'a') returns 1.0

-- NOTE: Snowflake is more forgiving for unsturcture access so try_xx versions will just use the 
-- element_at versions
CREATE OR REPLACE FUNCTION element_at(arr ARRAY, idx INTEGER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN idx > 0 THEN arr[idx - 1]       -- Snowflake usa índice base 0
        WHEN idx < 0 THEN arr[array_size(arr) + idx]  -- Soporta índices negativos
        ELSE NULL
    END
$$;


CREATE OR REPLACE FUNCTION element_at(ARR VARIANT, idx INTEGER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    CASE 
        WHEN idx > 0 THEN arr[idx - 1]       -- Snowflake uses 0 based indexes so we have to adjust
        WHEN idx < 0 THEN arr[array_size(arr) + idx]  -- Supports negative indexes
        ELSE NULL
    END
$$;

CREATE OR REPLACE FUNCTION element_at(OBJ OBJECT, KEY STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    GET(OBJ,KEY)
$$;

CREATE OR REPLACE FUNCTION try_element_at(arr ARRAY, idx INTEGER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    ELEMENT_AT(arr,idx)
$$;


CREATE OR REPLACE FUNCTION try_element_at(arr VARIANT, idx INTEGER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    ELEMENT_AT(arr,idx)
$$;



CREATE OR REPLACE FUNCTION try_element_at(obj OBJECT, key STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    element_at(obj, key)
$$;


-- SELECT element_at([1, 2, 3], -1); -- RETURNS 3

-- SELECT element_at([1, 2, 3], 5); -- RETURNS NULL

-- SELECT element_at(object_construct('1', 'a', '2', 'b'), '2'); -- returns b

-- SELECT element_at(object_construct('1', 'a', '2', 'b'), '3'); --returns null
