package com.snowflake.snowpark_java.stub;

import java.util.Iterator;

/**
 * Base interface for a map function used in GroupedDataset's mapGroup function.
 */
@Deprecated
@FunctionalInterface
public interface MapGroupsFunction<K, V, R>  {
  R call(K key, Iterator<V> values) throws Exception;
}