CREATE OR REPLACE FUNCTION REGEXP_REPLACEALL_UDF(INPUT STRING, PATTERN STRING,REPLACEMENT STRING)
RETURNS STRING
LANGUAGE JAVA
RUNTIME_VERSION = 11
handler='RegExpUtils.regexpReplace'
target_path='@~/regexp_replace.jar'
as
$$
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RegExpUtils {
    public static String regexpReplace(String input, String pattern, String replacement) {
        return input.replaceAll(pattern, replacement);
    }
}
$$;

-- SELECT REGEXP_REPLACEALL_UDF('AABBCCAA','AA','XX')
