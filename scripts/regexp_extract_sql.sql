-- A SQL equivalent for regexp_extract

CREATE OR REPLACE FUNCTION regexp_extract(str STRING, regexpr STRING, IDX INT DEFAULT 1) 
RETURNS STRING
LANGUAGE SQL AS
$$
    regexp_substr(str, regexpr, 1, 1,'e',idx)
$$;

-- 1. Extracting a simple pattern
SELECT regexp_extract('hello world', 'hello', 0) AS extracted_string;
-- This will extract the string 'hello' from 'hello world'.

-- 2. Extracting a pattern with a capturing group:
SELECT regexp_extract('2024-05-08', '(\\d{4})-(\\d{2})-(\\d{2})', 1) AS year;
-- This will extract the year '2024' from the date '2024-05-08'.

-- 3. Extracting part of a URL:
SELECT regexp_extract('https://www.example.com/page1', 'https://www\\.example\\.com/(\\w+)', 1) AS page;
This will extract the page name 'page1' from the URL.

-- 4. Extracting an email domain:


SELECT regexp_extract('user@example.com', '@(\\w+\\.\\w+)', 1) AS domain;

-- This will extract the domain 'example.com' from the email address.
-- 5. Extracting digits from a string:

SELECT regexp_extract('abc123def456', '(\\d+)', 0) AS extracted_digits;
-- This will extract the digits '123' from the string 'abc123def456'.
