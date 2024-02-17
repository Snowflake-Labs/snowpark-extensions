package com.snowflake.snowpark_java.stub;

/**
 * Base interface for function used in Dataset's reduce.
 */
@FunctionalInterface
public interface ReduceFunction<T>  {
  T call(T v1, T v2) throws Exception;
}
