-- Syntax: DATE_SUB(start_date, num_days)
-- 
-- Arguments:
-- 
-- start_date: A DATE expression representing the start date.
-- num_days: An integer expression representing the number of days.
-- Returns: A DATE that is num_days before start_date.

CREATE OR REPLACE FUNCTION DATE_SUB(start_date DATE,num_days NUMBER) RETURNS DATE IMMUTABLE AS 
'
    DATEADD( DAY, num_days * -1, start_date )
';


CREATE OR REPLACE FUNCTION DATE_SUB(start_date TIMESTAMP,num_days NUMBER) RETURNS TIMESTAMP IMMUTABLE AS 
'
    DATEADD( DAY, num_days * -1, start_date )
';

select DATE_SUB('2001-01-11',10);
-- Output: 2001-01-01

select DATE_SUB('2001-01-11'::TIMESTAMP,10);
-- Output: 2001-01-01 00:00:00.000
