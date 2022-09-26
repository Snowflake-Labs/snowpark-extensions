package com.snowflake.snowpark_extensions.implicits.Spark

import com.snowflake.snowpark.functions.lit
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

object DataFrameExtensionsSpark {

  // Init
  val session = SessionInitializer.spark
  val df = session
    .createDataFrame(DataFrameCreator.data_for_general)
    .toDF(DataFrameCreator.data_for_general_column: _*)
  val df_for_cast = session
    .createDataFrame(DataFrameCreator.data_for_date_cast)
    .toDF(DataFrameCreator.data_for_date_cast_column: _*)
  val df_for_order = session
    .createDataFrame(DataFrameCreator.data_for_order)
    .toDF(DataFrameCreator.data_for_order_column: _*)
  val df_for_json = session
    .createDataFrame(DataFrameCreator.data_for_json)
    .toDF(DataFrameCreator.data_for_json_column: _*)
  val df_union_all = session
    .createDataFrame(DataFrameCreator.data_for_union_all)
    .toDF(DataFrameCreator.data_for_union_all_column: _*)

  // Process
  def test_withColumnRenamed(): DataFrame ={
    df.withColumnRenamed("col1", "a")
  }

  def test_filterSQLExpression(): DataFrame = {
    df.filter("col2 > 1.2")
  }

  def test_filterSQLExpressionComplex(): DataFrame = {
    df.filter("col1 < 10 AND abs(col2 - round(col2, 0)) = 0.1 AND col3 IS NOT NULL")
  }

  def test_selectExpr(): DataFrame = {
    df.selectExpr("col2 + 8.8", "case when col1 = 1 then 'One' ELSE 'Two' END as col1_text")
  }

  def test_selectExprWithCast(): DataFrame = {
    df_for_cast.selectExpr("to_date(col5, 'yyyy-MM-dd') as date_only", "to_timestamp(col6, 'MM/dd/yyyy HH:mm:ss') as date_time")
  }

  def test_head() = {
    df.head().toSeq
  }

  def test_headN() = {
    df.head(5).map(_.toSeq)
  }

  def test_cache(): DataFrame = {
    val cachedDF = df.selectExpr("*", "col1 + 5 as test").cache()
    cachedDF.select("*")
  }

  def test_orderByColsSimple(): DataFrame = {
    df_for_order.orderBy(col("col1"))
  }

  def test_orderByColsExpression(): DataFrame = {
    df_for_order.orderBy(col("col1") % 2, col("col2").desc_nulls_last)
  }

  def test_orderBySQLExpression(): DataFrame = {
    df_for_order.orderBy("col1", "col2")
  }

  def test_take() = {
    df_for_order.take(7).map(_.toSeq)
  }

  def test_transformSimple(): DataFrame = {
    def helper_simpleTransform(df: DataFrame): DataFrame = {
      df.selectExpr("*", "col1 + 5 as test")
        .filter("test >= 7")
    }
    df.transform(helper_simpleTransform)
  }

  def test_transformChained(): DataFrame = {
    def helper_transform_withParams(value: Int)(df: DataFrame): DataFrame = {
      df
        .withColumn("test", col("col1") + value)
        .filter(col("test") >= value + 2)
    }
    test_transformSimple().transform(helper_transform_withParams(5))
  }

  def test_transformWithParams(): DataFrame = {
    def helper_transform_withParams(value: Int)(df: DataFrame): DataFrame = {
      df
        .withColumn("test", col("col1") + value)
        .filter(col("test") >= value + 2)
    }
    df.transform(helper_transform_withParams(5))
  }

  def test_toJSON(): DataFrame = {
    df_for_json.toJSON.toDF()
  }

  def test_collectAsList(): java.util.List[Row] = {
    df.collectAsList()
  }


  //Main
  def main(args: Array[String]): Unit = {
    // Spark testing main

    /*var a = test_concat_ws()
    a.show()*/
  }

}
