CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING) returns string as
$$
    coalesce(expression1,'')
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7),separator)
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8),separator)
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING, expression9 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9),separator)
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10),separator)
$$;


-- 12 arguments (separator + 11 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11), separator) 
$$;

-- 13 arguments (separator + 12 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12), separator) 
$$;

-- 14 arguments (separator + 13 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13), separator) 
$$;

-- 15 arguments (separator + 14 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14), separator) 
$$;

-- 16 arguments (separator + 15 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15), separator) 
$$;

-- 17 arguments (separator + 16 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16), separator) 
$$;

-- 18 arguments (separator + 17 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17), separator) 
$$;

-- 19 arguments (separator + 18 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18), separator) 
$$;

-- 20 arguments (separator + 19 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19), separator) 
$$;

-- 21 arguments (separator + 20 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20), separator) 
$$;

-- 22 arguments (separator + 21 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING, expression21 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20, expression21), separator) 
$$;

-- 23 arguments (separator + 22 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING, expression21 STRING, expression22 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20, expression21, expression22), separator) 
$$;

-- 24 arguments (separator + 23 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING, expression21 STRING, expression22 STRING, expression23 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20, expression21, expression22, expression23), separator) 
$$;

-- 25 arguments (separator + 24 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING, expression21 STRING, expression22 STRING, expression23 STRING, expression24 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20, expression21, expression22, expression23, expression24), separator) 
$$;

-- 26 arguments (separator + 25 expressions)
CREATE OR REPLACE FUNCTION CONCAT_WS(separator STRING, expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING, expression11 STRING, expression12 STRING, expression13 STRING, expression14 STRING, expression15 STRING, expression16 STRING, expression17 STRING, expression18 STRING, expression19 STRING, expression20 STRING, expression21 STRING, expression22 STRING, expression23 STRING, expression24 STRING, expression25 STRING) 
RETURNS STRING AS 
$$ 
  array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10, expression11, expression12, expression13, expression14, expression15, expression16, expression17, expression18, expression19, expression20, expression21, expression22, expression23, expression24, expression25), separator) 
$$;



CREATE OR REPLACE FUNCTION _CONCAT_WS_ARRAY(separator STRING,data ARRAY) returns string as
$$
    array_to_string(array_compact(data),separator)
$$;
