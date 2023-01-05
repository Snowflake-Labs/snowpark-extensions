package com.snowflake.snowpark_extensions.helpers.Snowpark

import com.snowflake.snowpark.{DataFrame, Window}
import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types.FloatType
import com.snowflake.snowpark_extensions.helpers.BuiltinOverrides.{avg, concat, concat_ws, lead, lag, approx_count_distinct,degrees,radians,ntile,atan2,acos}
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}

object BuiltinOverridesSnowpark {

  // Init
  val session = SessionInitializer.snow
  val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column)
  // -------------------------------------------------------
  // |"COL1"  |"COL2"  |"COL3"                    |"COL4"  |
  // -------------------------------------------------------
  // |1       |1.1     |NULL                      |c       |
  // |2       |2.1     |two                       |c       |
  // |237     |237.1   |two hundred thirty seven  |g       |
  // -------------------------------------------------------
  val df_window = session.createDataFrame(DataFrameCreator.data_for_window).toDF(DataFrameCreator.data_for_window_column)
  val df_double = session.createDataFrame(DataFrameCreator.data_for_double2).toDF(DataFrameCreator.data_for_double2_column)

  // Process
  def test_concat(): DataFrame = {
    val res = df.select(concat(col("col3"), col("col4")).as("result"))
    res.collect()
    // -----------------------------
    // |"RESULT"                   |
    // -----------------------------
    // |c                          |
    // |twoc                       |
    // |two hundred thirty seveng  |
    // -----------------------------
    res
  }

  def test_concat_ws(): DataFrame = {
    df.select(concat_ws("-", col("col3"), col("col4")).as("result"))
    // ------------------------------
    // |"RESULT"                    |
    // ------------------------------
    // |c                           |
    // |two-c                       |
    // |two hundred thirty seven-g  |
    // ------------------------------    
  }

  //avg
  def test_avg() : DataFrame = {
    df_window.select(avg(col("col1")).cast(FloatType).alias("mycol1"), avg(col("col2")).cast(FloatType).alias("mycol2"))
// -----------------------
// |"MYCOL1"  |"MYCOL2"  |
// -----------------------
// |1.5       |16.0      |
// -----------------------
  }

  //avgString
  def test_avgString() : DataFrame = {
    df_window.select(avg("col1").cast(FloatType).alias("mycol1"), avg("col2").cast(FloatType).alias("mycol2"))
// -----------------------
// |"MYCOL1"  |"MYCOL2"  |
// -----------------------
// |1.5       |16.0      |
// -----------------------
  }

  //lead
  def test_lead(): DataFrame = {
    val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
    df_window.withColumn("mycol", lead(col("col2"),1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
// -----------------------------
// |"COL1"  |"COL2"  |"MYCOL"  |
// -----------------------------
// |1       |10      |11       |
// |1       |11      |12       |
// |1       |12      |NULL     |
// |2       |20      |21       |
// |2       |21      |22       |
// |2       |22      |NULL     |
// -----------------------------
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
    df_window.withColumn("mycol",ntile(2) over (partitionWindow)).orderBy(col("col1"),col("col2"))
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
    // Snowpark testing main
  print("concat")
  test_concat.show
  print("concat_ws")
  test_concat_ws.show
  print("avg")
  test_avg.show
  print("avgString")
  test_avgString.show
  print("lead")
  test_lead.show
  print("leadString")
  test_leadString.show
  print("leadDefault")
  test_leadDefault.show
  print("leadDefaultString")
  test_leadDefaultString.show
  print("lag")
  test_lag.show
  print("lagString")
  test_lagString.show
  print("lagDefault")
  test_lagDefault.show
  print("lagDefaultString")
  test_lagDefaultString.show
  print("aprox_count_distinct")
  test_approx_count_distinct.show
  print("approx_count_distinctString")
  test_approx_count_distinctString.show
  print("degrees")
  test_degrees.show
  print("degreesString")
  test_degreesString.show
  print("radians")
  test_radians.show
  print("radiansString")
  test_radiansString.show
  print("ntile")
  test_ntile.show
  print("atan2")
  test_atan2.show
  print("atan2strCol")
  test_atan2strCol.show
  print("atan2colStr")
  test_atan2colStr.show
  print("atan2str")
  test_atan2str.show
  print("atan2colDbl")
  test_atan2colDbl.show
  print("atan2strDbl")
  test_atan2strDbl.show
  print("atan2dblCol")
  test_atan2dblCol.show
  print("atan2dblStr")
  test_atan2dblStr.show
  print("acos")
  test_acos.show
  print("acosStr")
  test_acosStr.show
  }

}
