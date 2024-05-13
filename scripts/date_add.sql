-- Syntax: DATE_ADD(start_date, num_days)
-- 
-- Arguments:
-- 
-- start_date: A DATE or TIMESTAMP expression representing the start date.
-- num_days: An integer expression representing the number of days.
-- Returns: A DATE that is num_days from start_date.

CREATE OR REPLACE FUNCTION DATE_ADD(start_date DATE,num_days NUMBER) RETURNS DATE IMMUTABLE AS 
'
    DATEADD( DAY, num_days, start_date )
';


CREATE OR REPLACE FUNCTION DATE_ADD(start_date TIMESTAMP,num_days NUMBER) RETURNS TIMESTAMP IMMUTABLE AS 
'
    DATEADD( DAY, num_days, start_date )
';

select DATE_ADD('2001-01-01',10);
-- Output: 2001-01-11

select DATE_ADD('2001-01-01'::TIMESTAMP,10);
-- Output: 2001-01-11 00:00:00.000
