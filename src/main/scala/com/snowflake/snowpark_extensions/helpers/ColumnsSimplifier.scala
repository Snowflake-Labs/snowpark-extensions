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

import com.snowflake.snowpark.{Column, DataFrame}

import scala.collection.mutable.ArrayBuffer;

/** Column simplifier helper class. Abstracts some column operations for a dataframe. Applied automatically to a
 * dataframe when using the DataframeExtensions implicit class.
 *
 * Its purpose is to provide a helper to increase performance when performing multiple withColumn operations.
 * This because Snowpark performs these operations immediately when executing, contrary to Spark that performs the operations
 * when a .show(), .collect() or other functions are called.
 *
 * What this class do is collect a series of withColumn operations that should be applied to a DataFrame, and groups them
 * to execute them in bulk when the .endCols function is called.
 * */
class ColumnsSimplifier(df: DataFrame) {

  /**
   * Column names to add to the DataFrame.
   */
  private var columnNames = ArrayBuffer[String]()
  /**
   * Column objects to add to the DataFrame.
   */
  private var columnValues = ArrayBuffer[Column]()
  /**
   * Column names to drop from the DataFrame.
   */
  private var columnDrops = ArrayBuffer[String]()

  /**
   * Dummy function. No current functionality has been implemented.
   * @param str
   * @param str1
   * @return Nothing
   */
  def cacheAsParquet(str: String, str1: String) = ???

  /**
   * Adds columns to drop.
   * @param colNames Columns to drop.
   * @return The current ColumnsSimplifier instance.
   */
  def drop(colNames:String*) : ColumnsSimplifier = {
    columnDrops ++= colNames
    this
  }

  /**
   * Adds a column to be added into the DataFrame.
   * @param colName Name of the new column.
   * @param colValue Column object for the new column.
   * @return The current ColumnsSimplifier instance.
   */
  def withColumn(colName: String, colValue:Column) : ColumnsSimplifier = {
    columnNames += colName
    columnValues += colValue
    this
  }

  /**
   * Applies the expected operations to the DataFrame.
   * @return DataFrame with the column operations applied.
   */
  def endCols =
    if (columnDrops.length == 0)
      df.withColumns(columnNames, columnValues)
    else
      df.withColumns(columnNames, columnValues).drop(columnDrops)
}
