--- NOTE: This script does not replicate properly the HIVE concat_ws behaviour, consider _concat_ws instead

-- UDF for 1 argument
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  NVL(arg1, '')
$$;

-- UDF for 2 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''))
$$;

-- UDF for 3 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''))
$$;

-- UDF for 4 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''))
$$;

-- UDF for 5 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''))
$$;

-- UDF for 6 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING, arg6 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''), 
         IFF(NVL(arg5, '') != '' AND NVL(arg6, '') != '', sep, ''), NVL(arg6, ''))
$$;

-- UDF for 7 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING, arg6 STRING, arg7 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''), 
         IFF(NVL(arg5, '') != '' AND NVL(arg6, '') != '', sep, ''), NVL(arg6, ''), 
         IFF(NVL(arg6, '') != '' AND NVL(arg7, '') != '', sep, ''), NVL(arg7, ''))
$$;

-- UDF for 8 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING, arg6 STRING, arg7 STRING, arg8 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''), 
         IFF(NVL(arg5, '') != '' AND NVL(arg6, '') != '', sep, ''), NVL(arg6, ''), 
         IFF(NVL(arg6, '') != '' AND NVL(arg7, '') != '', sep, ''), NVL(arg7, ''), 
         IFF(NVL(arg7, '') != '' AND NVL(arg8, '') != '', sep, ''), NVL(arg8, ''))
$$;

-- UDF for 9 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING, arg6 STRING, arg7 STRING, arg8 STRING, arg9 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''), 
         IFF(NVL(arg5, '') != '' AND NVL(arg6, '') != '', sep, ''), NVL(arg6, ''), 
         IFF(NVL(arg6, '') != '' AND NVL(arg7, '') != '', sep, ''), NVL(arg7, ''), 
         IFF(NVL(arg7, '') != '' AND NVL(arg8, '') != '', sep, ''), NVL(arg8, ''), 
         IFF(NVL(arg8, '') != '' AND NVL(arg9, '') != '', sep, ''), NVL(arg9, ''))
$$;

-- UDF for 10 arguments
CREATE OR REPLACE FUNCTION concat_ws_ignore_nulls(sep STRING, arg1 STRING, arg2 STRING, arg3 STRING, arg4 STRING, arg5 STRING, arg6 STRING, arg7 STRING, arg8 STRING, arg9 STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  CONCAT(NVL(arg1, ''), IFF(NVL(arg1, '') != '' AND NVL(arg2, '') != '', sep, ''), NVL(arg2, ''), 
         IFF(NVL(arg2, '') != '' AND NVL(arg3, '') != '', sep, ''), NVL(arg3, ''), 
         IFF(NVL(arg3, '') != '' AND NVL(arg4, '') != '', sep, ''), NVL(arg4, ''), 
         IFF(NVL(arg4, '') != '' AND NVL(arg5, '') != '', sep, ''), NVL(arg5, ''), 
         IFF(NVL(arg5, '') != '' AND NVL(arg6, '') != '', sep, ''), NVL(arg6, ''), 
         IFF(NVL(arg6, '') != '' AND NVL(arg7, '') != '', sep, ''), NVL(arg7, ''), 
         IFF(NVL(arg7, '') != '' AND NVL(arg8, '') != '', sep, ''), NVL(arg8, ''), 
         IFF(NVL(arg8, '') != '' AND NVL(arg9, '') != '', sep, ''), NVL(arg9, ''),
         IFF(NVL(arg9, '') != '' AND NVL(arg10, '') != '', sep, ''), NVL(arg10, ''),
         )
$$;
