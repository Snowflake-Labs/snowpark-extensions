package com.snowflake.snowpark_java.stub;

@Deprecated
@FunctionalInterface
public interface MapFunction<T, U>  {
  U call(T value) throws Exception;
}

