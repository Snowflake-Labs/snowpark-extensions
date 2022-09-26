package com.snowflake.snowpark_extensions.helpers.Snowpark

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}

object ColumnsSimplifierSnowpark {

  // Init
  val session = SessionInitializer.snow
  val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column)

  // Process
  def test_columns_simplifier(): DataFrame = {
    df.
      startCols.
      withColumn("a", col("col1")).
      withColumn("b", col("col2")).
      withColumn("c", col("col3")).
      withColumn("d", col("col4")).
      endCols
  }

  //Main
  def main(args: Array[String]): Unit = {
    // Snowpark testing main

    /*var a = test_concat_ws()
    a.show()*/
  }

}
