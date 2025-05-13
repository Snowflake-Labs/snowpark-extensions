-- This function does not implement yet any support for schema validation or options.

-- This functionality can be supported with a JS function but most cases only need somple JSON parsing

CREATE OR REPLACE FUNCTION JSON_PARSE(json_string STRING, schema STRING DEFAULT NULL, option OBJECT DEFAULT NULL )
RETURNS VARIANT 
LANGUAGE SQL
AS
$$
  TRY_PARSE_JSON(json_string)
$$;
