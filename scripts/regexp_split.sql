CREATE OR REPLACE FUNCTION public.regexp_split(input String, regex String, limit INT DEFAULT -1)
RETURNS ARRAY
LANGUAGE JAVA
RUNTIME_VERSION = '11'
PACKAGES = ('com.snowflake:snowpark:latest')
HANDLER = 'MyJavaClass.regex_split_run' 
AS
$$
import java.util.regex.Pattern;
public class MyJavaClass {
    public String[] regex_split_run(String input,String regex, int limit) {
        Pattern pattern = Pattern.compile(regex);
        return pattern.split(input, limit);
    }}$$;
