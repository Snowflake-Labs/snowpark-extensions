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

  /**
   * Wrapper for Snowflake built-in trim function. Trim the spaces from both ends for the specified string column.
   * @param column Column for trimming the spaces.
   * @return Column object.
   */
  def trim(column: Column): Column = com.snowflake.snowpark.functions.trim(column, lit(""))

  /**
   * Wrapper for Snowflake built-in rtrim function. Trim the spaces from right end for the specified string value.
   * @param column Column for trimming the spaces from right.
   * @return Column object.
   */
  def rtrim(column: Column): Column = com.snowflake.snowpark.functions.rtrim(column, lit(""))


  /**
   * Wrapper for Snowflake built-in ltrim function. Trim the spaces from left end for the specified string value.
   * @param column Column for trimming the spaces from left.
   * @return Column object.
   */
  def ltrim(column: Column): Column = com.snowflake.snowpark.functions.ltrim(column, lit(""))
  
  /**
   * Wrapper for Snowflake built-in round function. Splits str around matches of the given pattern.
   * @param column Column for spliting.
   * @param pattern String pattern for splitting..
   * @return Column object.
   */
  def split(column: Column, pattern: String): Column = com.snowflake.snowpark.functions.split(column, lit(pattern))

  /**
   * Wrapper for Snowflake built-in round function. Round the value of e to scale decimal places with HALF_UP round mode if scale is greater than or equal to 0 or at integral part when scale is less than 0.
   *
   * @param column Column for round the value.
   * @param scale Int scale decimal places.
   * @return Column object.
   */
  def round(column: Column, scale: Int): Column = com.snowflake.snowpark.functions.round(column, lit(scale))

  /**
   * Wrapper for Snowflake built-in repeat function. Repeats a string column n times, and returns it as a new string column.
   *
   * @param column Column for repeat.
   * @param n Int times for repeat.
   * @return Column object.
   */
  def repeat(column: Column, n: Int): Column = com.snowflake.snowpark.functions.repeat(column, lit(n))

  /**
   * Wrapper for Snowflake built-in translate function. Translate any character in the src by a character in replaceString.
   *
   * @param column Column for translate.
   * @param matchingString String for mach.
   * @param replaceString String for replacing.
   * @return Column object.
   */
  def translate(column: Column, matchingString: String, replaceString: String): Column = com.snowflake.snowpark.functions.translate(column, lit(matchingString), lit(replaceString))

  /**
   * Wrapper for Snowflake built-in next_day function. Returns the first date which is later than the value of the date column that is on the specified day of the week.
   *
   * @param date Column date to find the day.
   * @param dayOfWeek String day of the week you do want to find.
   * @return Column object.
   */
  def next_day(date: Column, dayOfWeek: String): Column = com.snowflake.snowpark.functions.next_day(date, lit(dayOfWeek))

}

