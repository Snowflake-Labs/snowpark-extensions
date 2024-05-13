-- Syntax: DATEDIFF(end_date, start_date)
-- 
-- Arguments:
-- 
-- end_date: A DATE or TIMESTAMP expression representing the end date.
-- start_date: A DATE or TIEMSTAMP expression representing the start date.
-- Returns: An INTEGER
-- 
-- A positive value indicates end_date is after start_date.
-- A negative value indicates end_date is before start_date.
-- Computes the number of days between two DATE expressions. 
-- start_date and end_date represent the start and end dates respectively. 
-- The result is positive if end_date is after start_date, and negative if end_date is before start_date.


CREATE OR REPLACE FUNCTION _DATEDIFF(end_date TIMESTAMP, start_date TIMESTAMP) RETURNS NUMBER IMMUTABLE AS '
    datediff(day,end_date,start_date)
';

-- SELECT '2022-11-01' A, '2022-12-07' B, _DATEDIFF(A, B) diff_in_days
-- union
-- SELECT '2022-12-01' A, '2022-12-07' B, _DATEDIFF(A, B) diff_in_days
-- union
-- SELECT '2022-12-02' A, '2022-12-07' B, _DATEDIFF(A, B) diff_in_days
-- union
-- SELECT '2022-12-03' A, '2022-12-07' B, _DATEDIFF(A, B) diff_in_days;
-- Output
-- A          | B          | DIFF_IN_DAYS
-- 2022-11-01 | 2022-12-07 | 36
-- 2022-12-01 | 2022-12-07 | 6
-- 2022-12-02 | 2022-12-07 | 5
-- 2022-12-03 | 2022-12-07 | 4
