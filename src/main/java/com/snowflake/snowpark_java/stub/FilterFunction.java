package com.snowflake.snowpark_java.stub;

/**
 * Base interface for a function used in Dataset's filter function.
 *
 * If the function returns true, the element is included in the returned Dataset.
 */
@Deprecated
@FunctionalInterface
public interface FilterFunction<T>  {
  boolean call(T value) throws Exception;
}
