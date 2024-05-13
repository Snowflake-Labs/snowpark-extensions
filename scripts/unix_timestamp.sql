-- UNIX_TIMESTAMP([date_or_time_expr [ , format_str ] ] )
-- 
-- Arguments:
-- 
-- date_or_time_expr: An optional expression evaluating to a DATE, TIMESTAMP, or STRING in a valid datetime format.
-- format_str: An optional STRING specifying the format of date_or_time_expr if it is a STRING.
-- Returns: A BIGINT.
-- 
-- If no argument is passed, the current timestamp is used.
-- format_str is ignored if date_or_time_expr evaluates to a DATE or TIMESTAMP.
-- If date_or_time_expr is a STRING, format_str is used to convert it to a TIMESTAMP before calculating the Unix timestamp. The default format_str value is 'YYYY-MM-DD HH:MM:SS'.
-- See the documentation for valid date and time format patterns (https://docs.snowflake.com/en/user-guide/date-time-input-output#about-the-elements-used-in-input-and-output-formats)
-- Invalid format_str or date_or_time_expr values will raise an error.





CREATE OR REPLACE FUNCTION UNIX_TIMESTAMP() RETURNS NUMBER IMMUTABLE AS 
'
    date_part(epoch_second,CURRENT_TIMESTAMP)
';

-- To get the UNIX timestamp for the current time:
-- SELECT CURRENT_TIMESTAMP,UNIX_TIMESTAMP();


CREATE OR REPLACE FUNCTION UNIX_TIMESTAMP(date_or_time_expr TIMESTAMP) RETURNS NUMBER IMMUTABLE AS 
'
    date_part(epoch_second,date_or_time_expr)
';

-- To get the UNIX timestamp for a specific date and time (using date_or_time_expr parameter):
-- SELECT UNIX_TIMESTAMP('2022-05-20 15:30:00');
-- Output: 1653060600

CREATE OR REPLACE FUNCTION UNIX_TIMESTAMP(date_or_time_expr STRING, format_str STRING) RETURNS NUMBER IMMUTABLE AS 
'
    date_part(epoch_second,TO_TIMESTAMP(date_or_time_expr,format_str))
';
-- To get the UNIX timestamp for a specific date and time with a custom format 
-- SELECT UNIX_TIMESTAMP('05/20/2022 3:30 PM', 'MM/dd/yyyy HH12:MI AM');
-- Output: 1653060600
