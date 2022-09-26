/**
    Copyright (C) Mobilize.Net info@mobilize.net - All Rights Reserved

    This file is part of the Mobilize Frameworks, which is
    proprietary and confidential.

    NOTICE:  All information contained herein is, and remains
    the property of Mobilize.Net Corporation.
    The intellectual and technical concepts contained herein are
    proprietary to Mobilize.Net Corporation and may be covered
    by U.S. Patents, and are protected by trade secret or copyright law.
    Dissemination of this information or reproduction of this material
    is strictly forbidden unless prior written permission is obtained
    from Mobilize.Net Corporation.
*/
package com.snowflake.snowpark_extensions.helpers

import com.snowflake.snowpark.Column
import com.snowflake.snowpark.functions.{array_compact, array_construct, array_to_string, col, lit}

/** Object that contains override for Snowflake built-in functions that do not have the same behavior as Spark. */
object BuiltinOverrides {

  /**
   * Concatenates columns from the parameters.
   * @param cols sequence of column objects to concatenate.
   * @return new column with concatenated values.
   */
  def concat(cols: Column*) = {
    array_to_string(array_compact(array_construct(cols: _*)),lit(""))
  }

  /**
   * Concatenates columns from the parameters using the specified character as a separator.
   * @param separator value with which the column's values will be separated.
   * @param cols sequence of column objects to concatenate.
   * @return new column with concatenated values separated by the specified separator.
   */
  def concat_ws(separator: String, cols: Column*) = {
    array_to_string(array_compact(array_construct(cols: _*)),lit(separator))
  }

  /**
   * Concatenates columns from the parameters using the specified character as a separator.
   * @param separator column with which the column's values will be separated.
   * @param cols sequence of column objects to concatenate.
   * @return new column with concatenated values separated by the specified separator.
   */
  def concat_ws(separator: Column, cols: Column*) = {
    array_to_string(array_compact(array_construct(cols: _*)),separator)
  }

  /**
   * Wrapper for Snowflake built-in avg function. Returns the average of the values in a group.
   * @param c Column to get the average.
   * @return Column object.
   */
  def avg(c: Column) = com.snowflake.snowpark.functions.avg(c)

  /**
   * Wrapper for Snowflake built-in avg function. Returns the average of the values in a group.
   * @param s Column name to get the average.
   * @return Column object.
   */
  def avg(s: String) = com.snowflake.snowpark.functions.avg(col(s))

  /**
   * Wrapper for Snowflake built-in lead function. Returns the value that is offset rows after the current row.
   * @param columnName Column name.
   * @param offset Column offset.
   * @return Column object.
   */
  def lead(columnName: String, offset: Int): Column =  com.snowflake.snowpark.functions.lead(col(columnName), offset)

  /**
   * Wrapper for Snowflake built-in lead function. Returns the value that is offset rows after the current row.
   * @param c Column to calculate the offset.
   * @param offset Column offset.
   * @return Column object.
   */
  def lead(c: Column, offset: Int): Column = com.snowflake.snowpark.functions.lead(c, offset)

  /**
   * Wrapper for Snowflake built-in lead function. Returns the value that is offset rows after the current row.
   * @param columnName Column name.
   * @param offset Column offset.
   * @param defaultValue If there is less than offset rows after the current row.
   * @return Column object.
   */
  def lead(columnName: String, offset: Int, defaultValue: Any): Column = com.snowflake.snowpark.functions.lead(col(columnName), offset, lit(defaultValue))

  /**
   * Wrapper for Snowflake built-in lead function. Returns the value that is offset rows after the current row.
   * @param c Column to calculate offset.
   * @param offset Column offset.
   * @param defaultValue If there is less than offset rows after the current row.
   * @return Column object.
   */
  def lead(c: Column, offset: Int, defaultValue: Any): Column = com.snowflake.snowpark.functions.lead(c, offset, lit(defaultValue))

  /**
   * Wrapper for Snowflake built-in lag function. Returns the value that is offset rows before the current row.
   * @param c Column to calculate offset.
   * @param offset Column offset.
   * @return Column object.
   */
  def lag(c: Column, offset: Int): Column = com.snowflake.snowpark.functions.lag(c, offset)

  /**
   * Wrapper for Snowflake built-in lag function. Returns the value that is offset rows before the current row.
   * @param columnName Column name.
   * @param offset Column offset.
   * @return Column object.
   */
  def lag(columnName: String, offset: Int): Column = com.snowflake.snowpark.functions.lag(col(columnName), offset)

  /**
   * Wrapper for Snowflake built-in lag function. Returns the value that is offset rows before the current row.
   * @param columnName Column name.
   * @param offset Column offset.
   * @param defaultValue If there is less than offset rows before the current row.
   * @return Column object.
   */
  def lag(columnName: String, offset: Int, defaultValue: Any): Column = com.snowflake.snowpark.functions.lag(col(columnName), offset, lit(defaultValue))

  /**
   * Wrapper for Snowflake built-in lag function. Returns the value that is offset rows before the current row.
   * @param c Column to calculate offset.
   * @param offset Column offset.
   * @param defaultValue If there is less than offset rows before the current row.
   * @return Column object.
   */
  def lag(c: Column, offset: Int, defaultValue: Any): Column = com.snowflake.snowpark.functions.lag(c, offset, lit(defaultValue))

  /**
   * Wrapper for Snowflake built-in approx_count_distinct function. Returns the approximate number of distinct items in a group.
   * @param c Column to get the count.
   * @return Column object.
   */
  def approx_count_distinct(c: Column): Column = com.snowflake.snowpark.functions.approx_count_distinct(c)

  /**
   * Wrapper for Snowflake built-in approx_count_distinct function. Returns the approximate number of distinct items in a group.
   * @param columnName Column name to get the count.
   * @return Column object.
   */
  def approx_count_distinct(columnName: String): Column = com.snowflake.snowpark.functions.approx_count_distinct(col(columnName))

  /**
   * Wrapper for Snowflake built-in degrees function. Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   * @param c Column for convert to degrees.
   * @return Column object.
   */
  def degrees(c: Column): Column = com.snowflake.snowpark.functions.degrees(c)

  /**
   * Wrapper for Snowflake built-in degrees function. Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   * @param columnName Column name for convert to degrees.
   * @return Column object.
   */
  def degrees(columnName: String): Column = com.snowflake.snowpark.functions.degrees(col(columnName))

  /**
   * Wrapper for Snowflake built-in radians function. Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   * @param c Column for convert to radians.
   * @return Column object.
   */
  def radians(c: Column): Column = com.snowflake.snowpark.functions.radians(c)

  /**
   * Wrapper for Snowflake built-in radians function. Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   * @param columnName Column name for convert to radians.
   * @return Column object.
   */
  def radians(columnName: String): Column = com.snowflake.snowpark.functions.radians(col(columnName))

  /**
   * Wrapper for Snowflake built-in ntile function. Returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
   * @param n ntile group.
   * @return Column object.
   */
  def ntile(n: Int): Column = com.snowflake.snowpark.functions.ntile(lit(n))

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param y coordinate on y-axis.
   * @param x coordinate on x-axis
   * @return Column object.
   */
  def atan2(y: Column, x: Column): Column = com.snowflake.snowpark.functions.atan2(y,x)

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param yName coordinate on y-axis.
   * @param x coordinate on x-axis
   * @return Column object.
   */
  def atan2(yName: String, x: Column): Column = com.snowflake.snowpark.functions.atan2(col(yName),x)

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param y coordinate on y-axis.
   * @param xName coordinate on x-axis
   * @return Column object.
   */
  def atan2(y: Column, xName: String): Column = com.snowflake.snowpark.functions.atan2(y,col(xName))

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param yName coordinate on y-axis.
   * @param xName coordinate on x-axis
   * @return Column object.
   */
  def atan2(yName: String, xName: String): Column = com.snowflake.snowpark.functions.atan2(col(yName),col(xName))

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param y coordinate on y-axis.
   * @param xValue coordinate on x-axis
   * @return Column object.
   */
  def atan2(y: Column, xValue: Double): Column = com.snowflake.snowpark.functions.atan2(y,lit(xValue))

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param yName coordinate on y-axis.
   * @param xValue coordinate on x-axis
   * @return Column object.
   */
  def atan2(yName: String, xValue: Double): Column = com.snowflake.snowpark.functions.atan2(col(yName),lit(xValue))

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param yValue coordinate on y-axis.
   * @param x coordinate on x-axis
   * @return Column object.
   */
  def atan2(yValue: Double, x: Column): Column = com.snowflake.snowpark.functions.atan2(lit(yValue),x)

  /**
   * Wrapper for Snowflake built-in atan2 function. Returns the theta component of the point (r, theta) in polar coordinates that corresponds to the point.
   * @param yValue coordinate on y-axis.
   * @param xName coordinate on x-axis
   * @return Column object.
   */
  def atan2(yValue: Double, xName: String): Column = com.snowflake.snowpark.functions.atan2(lit(yValue),col(xName))

  /**
   * Wrapper for Snowflake built-in acos function. Inverse cosine of e in radians.
   * @param e Column.
   * @return Column object.
   */
  def acos(e: Column): Column = com.snowflake.snowpark.functions.acos(e)

  /**
   * Wrapper for Snowflake built-in acos function. Inverse cosine of e in radians.
   * @param columnName Column name.
   * @return Column object.
   */
  def acos(columnName: String): Column = com.snowflake.snowpark.functions.acos(col(columnName))

}

