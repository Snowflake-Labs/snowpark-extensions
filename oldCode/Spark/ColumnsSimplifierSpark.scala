package com.snowflake.snowpark_extensions.helpers.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ColumnsSimplifierSpark {

    // Init
    val session = SessionInitializer.spark
    val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column:_*)

    // Process
    def test_columns_simplifier(): DataFrame = {
      df.
        withColumn("a", col("col1")).
        withColumn("b", col("col2")).
        withColumn("c", col("col3")).
        withColumn("d", col("col4"))
    }

    //Main
    def main(args: Array[String]): Unit = {
        // Spark testing main

        /*var a = test_concat_ws()
        a.show()*/
    }

}
