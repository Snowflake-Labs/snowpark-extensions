package com.snowflake.snowpark_java.stub;



import java.util.Iterator;



/**
 * A function that returns zero or more output records from each input record.
 */
@Deprecated
@FunctionalInterface
public interface FlatMapFunction<T, R>  {
  Iterator<R> call(T t) throws Exception;
}