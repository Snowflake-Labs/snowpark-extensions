CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING) returns string as
$$
    coalesce(expression1,'')
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, expression7 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7),'-')
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING, expression9 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9),'-')
$$;

CREATE OR REPLACE FUNCTION _CONCAT_WS(separator STRING,expression1 STRING, expression2 STRING, expression3 STRING, expression4 STRING, expression5 STRING, expression6 STRING, 
expression7 STRING, expression8 STRING, expression9 STRING, expression10 STRING) returns string as
$$
    array_to_string(array_construct_compact(expression1, expression2, expression3, expression4, expression5, expression6, expression7, expression8, expression9, expression10),'-')
$$;


CREATE OR REPLACE FUNCTION _CONCAT_WS_ARRAY(separator STRING,data ARRAY) returns string as
$$
    array_to_string(array_compact(data),'-')
$$;
