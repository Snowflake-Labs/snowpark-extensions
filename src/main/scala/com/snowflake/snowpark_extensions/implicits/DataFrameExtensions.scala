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
package com.snowflake.snowpark_extensions.implicits

import com.snowflake.snowpark.{Column, DataFrame, Row}
import com.snowflake.snowpark.functions._
import com.snowflake.snowpark_extensions.helpers.ColumnsSimplifier
import com.snowflake.snowpark.types._
import com.snowflake.snowpark_extensions.Extensions.extendedDataFrame

/** DataFrame Extensions object containing implicit functions to the Snowpark DataFrame object. */
object DataFrameExtensions {

  /**
   * DataFrame extension class.
   * @param df DataFrame to extend functionality.
   */
  class ExtendedDataFrame(df: DataFrame) {

    /**
     * Function that returns a Seq of strings with the DataFrame's column names.
     * @return list of columns in the DataFrame
     */
    def columns: Seq[String] = df.schema.map(x => x.name)

    /**
     * Column simplifier object to increase performance of withColumns functionality.
     * @return Column simplifier class
     */
    def startCols = new ColumnsSimplifier(df)

    /**
     * Function that returns the dataframe with a column renamed.
     * @param existingName Name of the column to rename.
     * @param newName New name to give to the column.
     * @return DataFrame with the column renamed.
     */
    def withColumnRenamed(existingName: String, newName: String): DataFrame = {
      df.rename(newName, col(existingName))
    }

    /**
     * Transforms the DataFrame according to the function from the parameter.
     * @param func Function to apply to the DataFrame.
     * @return DataFrame with the transformation applied.
     */
    def transform(func: DataFrame => DataFrame): DataFrame = func(df)

    /**
     * Overload of dropDuplicates to comply with Spark's implementations of dropDuplicates function. Unspecified columns
     * from the dataframe will be preserved, but won't be considered to calculate duplicates. For rows with different
     * values on unspecified columns, it will return the first row.
     * @param columns List of columns to group by to detect the duplicates.
     * @return DataFrame without duplicates on the specified columns.
     */
    def dropDuplicates(columns: Seq[String]): DataFrame = {
      df.dropDuplicates(columns: _*)
    }

    /**
     * Overload of filter to comply with Spark's implementation of filter function when receiving a SQL expression.
     * @param conditionExpr SQL conditional expression to filter the dataset on it.
     * @return DataFrame filtered on the specified SQL expression.
     */
    def filter(conditionExpr: String): DataFrame = {
      df.where(sqlExpr(conditionExpr))
    }

    /**
     * Equivalent to Spark's selectExpr. Selects columns based on the expressions specified. They could either be
     * column names, or calls to other functions such as conversions, case expressions, among others.
     * @param exprs Expressions to apply to select from the DataFrame.
     * @return DataFrame with the selected expressions as columns. Unspecified columns are not included.
     */
    def selectExpr(exprs: String*): DataFrame = {
      df.select(exprs.map(e => sqlExpr(e)))
    }

    /**
     * Equivalent to Spark's head. Returns the first row. Since this is an Option element, a `.get` is required to get the actual row.
     * Spark's default behavior with Empty DataFrames throw an error when executing this function, however this behavior isn't replicated exactly.
     * @return The first row of the DataFrame.
     */
    def head(): Option[Row] = {
      df.first()
    }

    /**
     * Equivalent to Spark's head. Returns the first N rows.
     * Spark's default behavior with Empty DataFrames throw an error when executing this function, however this behavior isn't replicated exactly.
     * @param n Amount of rows to return.
     * @return Array with the amount of rows specified in the parameter.
     */
    def head(n: Int): Array[Row] = {
      df.first(n)
    }

    /**
     * Equivalent to Spark's take. Returns the first N rows.
     * Spark's implementation of this function differs from the one from head.
     * In paper they have the same functionality, but in practice they have different implementations since head is mostly used for returning small numbers
     * whereas take can be used for larger amounts of rows.
     * This function does not make a difference on implementation from head.
     * @param n Amount of rows to return.
     * @return Array with the amount of rows specified in the parameter.
     */
    def take(n: Int): Array[Row] = {
      df.first(n)
    }

    /**
     * Caches the result of the DataFrame and creates a new Dataframe, whose operations won't affect the original DataFrame.
     * @return New cached DataFrame.
     */
    def cache(): DataFrame = {
      df.cacheResult()
    }

    /**
     * Alias for Spark OrderBy function. Receives columns or column expressions.
     * @param sortExprs Column expressions to order the dataset by.
     * @return Returns the dataset ordered by the specified expressions
     */
    def orderBy(sortExprs: Column*): DataFrame = {
      df.sort(sortExprs.map(x => x))
    }

    /**
     * Alias for Spark OrderBy function. Receives column names. While testing Spark OrderBy, it doesn't accept SQL Expressions, only column names.
     * @param sortCol Column name 1
     * @param sortCols Variable column names
     * @return DataFrame filtered on the variable names.
     */
    def orderBy(sortCol: String, sortCols: String*): DataFrame = {
      df.sort((Seq(sortCol) ++ sortCols).map(s => col(s)))
    }

    /**
     * Alias for Spark printSchema. This is a shortcut to schema.printTreeString(). Prints the schema of the DataFrame in a tree format.
     * Includes column names, data types and if they're nullable or not. The results between this function and Spark's implementation is
     * not identical, but it is very similar.
     */
    def printSchema(): Unit = {
      df.schema.printTreeString()
    }

    /**
     * Implementation of Spark's toJSON function. Converts each row into a JSON object and returns a DataFrame with a single column.
     * @return DataFrame with 1 column whose value corresponds to a JSON object of the row.
     */
    def toJSON: DataFrame = {
      df.select(object_construct(col("*")).cast(StringType).as("value"))
    }

    /**
     * Implementation of Spark's collectAsList function. Collects the DataFrame and converts it to a java.util.List[Row] object.
     * @return A java.util.List[Row] representation of the DataFrame.
     */
    def collectAsList(): java.util.List[Row] = {
      java.util.Arrays.asList(df.collect():_*)
    }

    /**
     * Implementation of Sparks's unionByName allowing an union between dataframes that does not contains the exact same columns
     *
     * @param other               Dataframe to be united
     * @param allowMissingColumns If true, there is going to be additional logic to avoid error in Snowpark, if false there is no anything additional within expected Snowpark implementation (failing using dataframes with different columns)
     * @return New dataframe united both dataframes
     * @example
     * {{{
     *    val df = Seq((1, 2, 3)).toDF("col0", "col1", "col2")
     *    val other = Seq((4, 5, 6)).toDF("col1", "col2", "col3")
     *
     *    // Failing code for columns differentiation
     *    df.unionByName(other).show()
     *
     *    // Failing code for columns differentiation (as the allowMissingColumns flag is set as False)
     *    df.unionByName(other, allowMissingColumns=False).show()
     *
     *    // Working code, allowMissingColumns is set as True, so the missing columns are going to be filled with NULL
     *    df.unionByName(other, allowMissingColumns=True).show()
     *
     *    // -------------------------------------
     *    // |"COL0"  |"COL1"  |"COL2"  |"COL3"  |
     *    // -------------------------------------
     *    // |1       |2       |3       |NULL    |
     *    // |NULL    |4       |5       |6       |
     *    // -------------------------------------
     *
     * }}}
     *
     */
    def unionByName(other: DataFrame, allowMissingColumns: Boolean) = {
      if (allowMissingColumns) {
        val merged_cols = df.columns.toSet ++ other.columns.toSet

        def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
          merged_cols.toList.map(x => x match {
            case x if column.contains(x) => col(x)
            case _ => lit(null).as(x)
          })
        }

        val new_df1 = df.select(getNewColumns(df.columns.toSet, merged_cols))
        val new_df2 = other.select(getNewColumns(other.columns.toSet, merged_cols))

        new_df1.unionByName(new_df2)
      } else {
        df.unionByName(other)
      }
    }

  }
}
