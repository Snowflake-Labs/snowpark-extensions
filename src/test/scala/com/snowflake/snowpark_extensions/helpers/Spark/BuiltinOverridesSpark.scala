package com.snowflake.snowpark_extensions.helpers.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object BuiltinOverridesSpark {

    // Init
    val session = SessionInitializer.spark
    val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column:_*)
    val df_window = session.createDataFrame(DataFrameCreator.data_for_window).toDF(DataFrameCreator.data_for_window_column:_*)
    val df_double = session.createDataFrame(DataFrameCreator.data_for_double2).toDF(DataFrameCreator.data_for_double2_column:_*)

  // Process
    def test_concat() : DataFrame = {
        df.select(concat(col("col3"),col("col4")).as("result"))
    }
    def test_concat_ws(): DataFrame = {
        df.select(concat_ws("-", col("col3"),col("col4")).as("result"))
    }

    //avg
    def test_avg() : DataFrame = {
      df_window.select(avg("col1").cast(FloatType).alias("mycol1"), avg("col2").cast(FloatType).alias("mycol2"))
    }

    //lead
    def test_lead(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead(col("col2"),1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadString
    def test_leadString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead("col2",1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadDefault
    def test_leadDefault(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead(col("col2"),1,0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadDefaultString
    def test_leadDefaultString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead("col2",1, 0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lag
    def test_lag(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag(col("col2"),1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagString
    def test_lagString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag("col2",1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagDefault
    def test_lagDefault(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag(col("col2"),1,0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagDefaultString
    def test_lagDefaultString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag("col2",1, 0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //approx_count_distinct
    def test_approx_count_distinct() : DataFrame = {
      df_window.select(approx_count_distinct(col("col1")).alias("mycol1"), approx_count_distinct(col("col2")).alias("mycol2"))
    }

    //approx_count_distinctString
    def test_approx_count_distinctString() : DataFrame = {
      df_window.select(approx_count_distinct("col1").alias("mycol1"),approx_count_distinct("col2").alias("mycol2"))
    }

    //degrees
    def test_degrees() : DataFrame = {
      df_double.select(degrees(col("col1")).alias("mycol1"))
    }

    //degreesString
    def test_degreesString() : DataFrame = {
      df_double.select(degrees("col1").alias("mycol1"))
    }

    //radians
    def test_radians() : DataFrame = {
      df_double.select(radians(col("col1")).alias("mycol1"))
    }

    //radiansString
    def test_radiansString() : DataFrame = {
      df_double.select(radians("col1").alias("mycol1"))
    }

    //ntile
    def test_ntile() : DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", ntile(2) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //atan2col
    def test_atan2() : DataFrame = {
      df_double.select(atan2(col("col1"),col("col2")).alias("mycol1"))
    }
    //atan2
    def test_atan2strCol() : DataFrame = {
      df_double.select(atan2("col1",col("col2")).alias("mycol1"))
    }
    //atan2
    def test_atan2colStr() : DataFrame = {
      df_double.select(atan2(col("col1"),"col2").alias("mycol1"))
    }
    //atan2
    def test_atan2str() : DataFrame = {
      df_double.select(atan2("col1","col2").alias("mycol1"))
    }

    //atan2
    def test_atan2colDbl() : DataFrame = {
      df_double.select(atan2(col("col2"),3.1).alias("mycol1"))
    }

    //atan2
    def test_atan2strDbl() : DataFrame = {
      df_double.select(atan2("col2",3.1).alias("mycol1"))
    }

    //atan2
    def test_atan2dblCol() : DataFrame = {
      df_double.select(atan2(3.1,col("col2")).alias("mycol1"))
    }

    //atan2
    def test_atan2dblStr() : DataFrame = {
      df_double.select(atan2(3.1,"col2").alias("mycol1"))
    }

    //acos
    def test_acos() : DataFrame = {
      df_double.select(acos(col("col1")).alias("mycol1"))
    }

    //acosStr
    def test_acosStr() : DataFrame = {
      df_double.select(acos("col1").alias("mycol1"))
    }

  //Main
    def main(args: Array[String]): Unit = {
        // Spark testing main
      test_acosStr.show
    }

}
