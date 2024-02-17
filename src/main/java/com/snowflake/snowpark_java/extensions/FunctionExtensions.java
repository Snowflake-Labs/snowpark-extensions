package com.snowflake.snowpark_java.extensions;

import static com.snowflake.snowpark_java.Functions.sqlExpr;
import com.snowflake.snowpark_java.Functions;

import com.snowflake.snowpark_java.Column;

public class FunctionExtensions {

    public static Column count(String columnName) {
        return Functions.count(col(columnName));
    }
    
    public static Column max(String columnName) {
        return Functions.max(col(columnName));
    }

    public static Column sum(String columnName) {
        return Functions.sum(col(columnName));
    }

    public static Column expr(String expr) {
        return sqlExpr(expr);
    }
    public static Column substring(Column c,int pos, int len) {
        return Functions.substring(c, Functions.lit(pos), Functions.lit(len));
    }


    public static Column rand() {
        return Functions.uniform(Functions.lit(0.0), Functions.lit(1.0),Functions.callUDF("random"));
    }

    public static Column col(String col) {
        if (col.contains(".")) {
            String[] split = col.split("\\.");
            if (split.length != 2) {
                throw new IllegalArgumentException("Invalid column format");
            }
            String alias = split[0];
            String name = split[1];
            if (!Extensions.aliasMap.containsKey(alias)) {
                throw new IllegalArgumentException("Alias not found");
            }
            return Extensions.aliasMap.get(alias).col(name);
        }
        else
            return Functions.col(col);
    }

    public Column alias_col(String col) {
        String[] split = col.split("\\.");
        if (split.length != 2) {
            throw new IllegalArgumentException("Invalid column format");
        }
        String alias = split[0];
        String name = split[1];
        if (!Extensions.aliasMap.containsKey(alias)) {
            throw new IllegalArgumentException("Alias not found");
        }
        return Extensions.aliasMap.get(alias).col(name);
    }
}
