CREATE OR REPLACE FUNCTION REGEXP_EXTRACT_UDF(SEARCHSTRING STRING, PATTERN STRING,GROUP_NUMBER INTEGER)
RETURNS STRING
LANGUAGE JAVA
RUNTIME_VERSION = 11
handler='RegExpUtils.regexpExtract'
target_path='@~/regexp_extract.jar'
as
$$
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RegExpUtils {
    public static String regexpExtract(String searchString, String pattern, Integer group) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(searchString);
        
        if (m.find()) {
            return m.group(group);
        }
        else {
            return "";
        }
        
    }
}
$$;
-- SELECT REGEXP_EXTRACT_UDF('123M567','(\\d+)',1)
