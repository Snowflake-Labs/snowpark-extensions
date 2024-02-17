package com.snowflake.snowpark_java.extensions;

import com.snowflake.snowpark_java.SessionBuilder;
import com.snowflake.snowpark_java.stub.FilterFunction;
import com.snowflake.snowpark_java.stub.FlatMapFunction;
import com.snowflake.snowpark_java.stub.MapFunction;
import com.snowflake.snowpark_java.stub.MapGroupsFunction;
import com.snowflake.snowpark_java.stub.ReduceFunction;
import com.snowflake.snowpark_java.types.StructType;
import com.snowflake.snowpark_java.types.StructField;
import com.snowflake.snowpark_java.Column;
import com.snowflake.snowpark_java.DataFrame;
import com.snowflake.snowpark_java.Functions;
import com.snowflake.snowpark_java.Row;

import static com.snowflake.snowpark_java.Functions.lit;
import static com.snowflake.snowpark_java.Functions.sqlExpr;
import static com.snowflake.snowpark_java.Functions.col;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

public class Extensions {

    public static SessionBuilder from_snowsql(SessionBuilder builder) {
        try {
            Map<String,String> configs = BuilderExtensions.read_snowsql_config("connections");
            String accountname = configs.getOrDefault("accountname","missing");
            String url =  "https://" + accountname + ".snowflakecomputing.com";
            builder.config("URL", url);
            builder.config("USER",configs.get("username"));
            builder.config("PASSWORD",configs.get("password"));
            builder.config("ROLE",configs.get("rolename"));
            builder.config("DB",configs.get("dbname"));
            builder.config("SCHEMA",configs.get("schemaname"));
            builder.config("WAREHOUSE",configs.get("warehousename"));
            builder.configs(configs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return builder;
    }
    public static Map<String, DataFrame> aliasMap = new HashMap<>();

    /**
     * Equivalent to Spark's selectExpr. Selects columns based on the expressions specified. They could either be
     * column names, or calls to other functions such as conversions, case expressions, among others.
     * @param exprs Expressions to apply to select from the DataFrame.
     * @return DataFrame with the selected expressions as columns. Unspecified columns are not included.
     */
    public static DataFrame selectExpr(DataFrame df, String... exprs) 
    {
        List<Column> expressions = new ArrayList<Column>();
        for(String expr: exprs) {
            expressions.add(sqlExpr(expr));
        }
        return df.select(expressions.toArray(new Column[expressions.size()]));
    }

    public static String[] columns(DataFrame df) {
        return fieldNames(df.schema());
    }

    public static DataFrame flatMap(DataFrame df, FlatMapFunction<Row,Row> mapper, StructType struct) {
        throw new RuntimeException("Not implemented");
    }

    public static <T,R> DataFrame map(DataFrame df, MapFunction<T,R> mapper) {
        throw new RuntimeException("Not implemented");
    }

    public static <K,V,R> DataFrame mapGroups(DataFrame df, MapGroupsFunction<K,V,R> mapper, StructType structType) {
        throw new RuntimeException("Not implemented");
    }

    public static <T,R> DataFrame groupByKey(DataFrame df, MapFunction<T,R> mapper) {
        throw new RuntimeException("Not implemented");
    }

        public static <T,R> DataFrame groupByKey(DataFrame df, MapFunction<T,R> mapper, StructType structType) {
        throw new RuntimeException("Not implemented");
    }

      public static <R> DataFrame reduceGroups(DataFrame df, ReduceFunction<R> mapper) {
        throw new RuntimeException("Not implemented");
    }

    public static <T,R> DataFrame map(DataFrame df, MapFunction<T,R> mapper, StructType struct) {
        throw new RuntimeException("Not implemented");
    }
    public static String[] fieldNames(StructType struct) {
        ArrayList<String> fields = new ArrayList<String>();
        Iterator<StructField> iterator = struct.iterator();
        while(iterator.hasNext()) {
            StructField field = iterator.next();
            fields.add(field.name());
        }
        return fields.toArray(new String[fields.size()]);
    }
    public static DataFrame withColumnRenamed(DataFrame df, String newName, String oldName) {
        return df.rename(newName, col(oldName));
    }

    public static <T> DataFrame filter(DataFrame df, FilterFunction<T> filter) {
        throw new RuntimeException("Not implemented");
    }

    public static DataFrame filter(DataFrame df, Column filter) {
        return df.filter(filter);
    }

    public static DataFrame filter(DataFrame df, String filter) {
        return df.filter(Functions.sqlExpr(filter));
    }
    
    public static DataFrame as(DataFrame df, String alias) {
        aliasMap.put(alias, df);
        return df;
    }
    
    @Deprecated
    public static int fieldIndex( com.snowflake.snowpark_java.Row thiz, String colName) {
       
        throw new RuntimeException("The list of column is needed");
    }

     public static int fieldIndex( com.snowflake.snowpark_java.Row thiz, String[] columns, String colName) {
        for (int i =0;i<columns.length;i++) {
            if (columns[i] == colName)
            {
                return i;
            }
        }
        throw new RuntimeException("column " + colName + " not found");
    }

    

    public static Column startsWith( com.snowflake.snowpark_java.Column thiz, String literal) {
        return Functions.startswith(thiz, lit(literal));
    }

    public static Column as( com.snowflake.snowpark_java.Column thiz, String alias) {
        return thiz.alias(alias);
    }
    public static Column lt( com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.lt(lit(x));
    }
    public static Column lt( com.snowflake.snowpark_java.Column thiz, String x) {
        return thiz.lt(lit(x));
    }

    public static Column gt( com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.gt(lit(x));
    }

    public static Column multiply( com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.multiply(lit(x));
    }

    public static Column divide( com.snowflake.snowpark_java.Column thiz, int x) {
        return thiz.gt(lit(x));
    }

    public static Column otherwise( com.snowflake.snowpark_java.CaseExpr thiz, int x) {
        return thiz.otherwise(lit(x));
    }

    public static Column otherwise( com.snowflake.snowpark_java.CaseExpr thiz, Column x) {
        return thiz.otherwise(x);
    }

    public static Column eqNullSafe( com.snowflake.snowpark_java.Column thiz, Column other) {
        throw new RuntimeException("Not implemented");
    }

    public static Column notEqual( com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.not_equal(other);
    }

    public static Column notEqual( com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.not_equal(lit(other));
    }

        public static Column notEqual( com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.not_equal(lit(other));
    }

    public static Column notEqual( com.snowflake.snowpark_java.Column thiz, String other) {
        return thiz.not_equal(lit(other));
    }

        public static Column not_equal( com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.not_equal(lit(other));
    }

    public static Column substr(Column c,int pos, int len) {
        return Functions.substring(c, Functions.lit(pos), Functions.lit(len));
    }

    public static Column equalTo( com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.equal_to(lit(other));
    }

    public static Column equalTo( com.snowflake.snowpark_java.Column thiz, double other) {
        return thiz.equal_to(lit(other));
    }
    
    public static Column equalTo( com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.equal_to(other);
    }

    public static Column equalTo( com.snowflake.snowpark_java.Column thiz, String other) {
        return thiz.equal_to(lit(other));
    }

    public static Column gep( com.snowflake.snowpark_java.Column thiz, int other) {
        return thiz.geq(lit(other));
    }

    public static Column gep( com.snowflake.snowpark_java.Column thiz, Column other) {
        return thiz.geq(other);
    }

    public static Column isNull(com.snowflake.snowpark_java.Column thiz) {
        
        return thiz.is_null();
    }

    public static Column isNotNull(com.snowflake.snowpark_java.Column thiz) {
        
        return thiz.is_not_null();
    }

    // public static Column eqNullSafe(com.snowflake.snowpark_java.Column thiz) {
        
    //     return thiz.eq();
    // }

    public static Column isin(com.snowflake.snowpark_java.Column thiz,String... values) {
        return thiz.in((Object[])values);
    }

    public static Column between(com.snowflake.snowpark_java.Column thiz,String lowerBound, String upperBound) {
        return thiz.between(lit(lowerBound), lit(upperBound));
    }

    public static Column like(com.snowflake.snowpark_java.Column thiz,String pattern) {
        return thiz.like(lit(pattern));
    }



}