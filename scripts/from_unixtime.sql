
-- Returns a date string representation of a Unix epoch timestamp using the specified format.
-- 
-- Syntax: FROM_UNIXTIME(unix_timestamp [, format_str] )
-- 
-- Arguments:
-- 
-- unix_timestamp: An integer expression representing the seconds elapsed since unix epoch (1970-01-01 00:00:00 UTC)
-- format_str: An optional STRING specifying the format of the output date string.
-- Returns: A STRING in the format specified by format_str.
-- 
-- If omitted, the default format 'YYYY-MM-DD HH:MM:SS' is used.
-- Refer to the documentation for valid datetime format patterns (https://docs.snowflake.com/en/user-guide/date-time-input-output#about-the-elements-used-in-input-and-output-formats).

CREATE OR REPLACE FUNCTION FROM_UNIXTIME(unix_timestamp NUMBER) RETURNS STRING IMMUTABLE AS '
    TO_VARCHAR(TO_TIMESTAMP(unix_timestamp),''YYYY-MM-DD HH:MM:SS'')
';

-- SELECT FROM_UNIXTIME(0)
-- Outputs: 1970-01-01 00:01:00

CREATE OR REPLACE FUNCTION FROM_UNIXTIME(unix_timestamp NUMBER, format_str STRING) RETURNS STRING IMMUTABLE AS '
    TO_VARCHAR(TO_TIMESTAMP(unix_timestamp),format_str)
';

-- SELECT 
--     from_unixtime(1562007679),
--     from_unixtime(1562007679,'MM-dd-yyyy HH:mm:ss'),
--     from_unixtime(1561964400,'MM-dd-yyyy');
-- Outputs:
-- 2019-07-01 19:07:19
-- 07-01-2019 19:07:19
-- 07-01-2019
