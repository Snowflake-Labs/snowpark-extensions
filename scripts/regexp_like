CREATE OR REPLACE FUNCTION REGEXP_LIKE_UDF(SEARCHSTRING STRING, PATTERN STRING)
RETURNS BOOLEAN
LANGUAGE JAVA
RUNTIME_VERSION = 11
handler='RegExpUtils.regexpLike'
target_path='@~/regexp_like.jar'
as
$$
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RegExpUtils {
    public static Boolean regexpLike(String searchString, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(searchString);

        return m.matches();
    }
}
$$;

-- SELECT REGEXP_LIKE_UDF('AABBCCAA','AA.*')
