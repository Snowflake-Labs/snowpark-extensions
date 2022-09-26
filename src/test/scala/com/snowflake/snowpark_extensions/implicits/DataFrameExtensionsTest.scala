package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.implicits.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.{df2Seq, snowList2Seq, sparkList2Seq}
import org.scalatest.{FlatSpec, Matchers}
import com.snowflake.snowpark_extensions.implicits.Snowpark.{DataFrameExtensionsSnowpark => snow}
import com.snowflake.snowpark_extensions.implicits.Spark.{DataFrameExtensionsSpark => spark}

class DataFrameExtensionsTest extends FlatSpec with Matchers {
  behavior of "DataFrameExtensions class"

  "withColumnRenamed" should "match spark withColumnRenamed" in {
    df2Seq(spark.test_withColumnRenamed()) shouldEqual df2Seq(snow.test_withColumnRenamed())
  }

  "filter with SQL expression" should "match Spark filter with SQL expression" in {
    df2Seq(spark.test_filterSQLExpression()) shouldEqual df2Seq(snow.test_filterSQLExpression())
  }

  "filter with complex SQL expression" should "match Spark filter with complex SQL expression" in {
    df2Seq(spark.test_filterSQLExpressionComplex()) shouldEqual df2Seq(snow.test_filterSQLExpressionComplex())
  }

  "custom selectExpr" should "match Spark selectExpr" in {
    df2Seq(spark.test_selectExpr()) shouldEqual df2Seq(snow.test_selectExpr())
  }

  "custom selectExpr with Cast" should "match Spark selectExpr with Cast" in {
    df2Seq(spark.test_selectExprWithCast()) shouldEqual df2Seq(snow.test_selectExprWithCast())
  }

  "head(n)" should "match Spark head(n)" in {
    spark.test_headN() shouldEqual snow.test_headN()
  }

  "head" should "match Spark head" in {
    spark.test_head() shouldEqual snow.test_head()
  }

  "select on cached DF" should "match Spark select on cached DF" in {
    df2Seq(spark.test_cache()) shouldEqual df2Seq(snow.test_cache())
  }

  "simple column order by" should "match Spark simple order by" in {
    df2Seq(spark.test_orderByColsSimple()) shouldEqual df2Seq(snow.test_orderByColsSimple())
  }

  "column expressions order by" should "match Spark column expressions order by" in {
    df2Seq(spark.test_orderByColsExpression()) shouldEqual df2Seq(snow.test_orderByColsExpression())
  }

  "string expressions order by" should "match Spark string expressions order by" in {
    df2Seq(spark.test_orderBySQLExpression()) shouldEqual df2Seq(snow.test_orderBySQLExpression())
  }

  "take(n)" should "match Spark take(n)" in {
    spark.test_take() shouldEqual snow.test_take()
  }

  "simple transform" should "match Spark simple transform" in {
    df2Seq(spark.test_transformSimple()) shouldEqual df2Seq(snow.test_transformSimple())
  }

  "chained transform" should "match Spark chained transform" in {
    df2Seq(spark.test_transformChained()) shouldEqual df2Seq(snow.test_transformChained())
  }

  "transform with params" should "match Spark transform with params" in {
    df2Seq(spark.test_transformWithParams()) shouldEqual df2Seq(snow.test_transformWithParams())
  }

  "toJSON" should "match Spark toJSON" in {
    df2Seq(spark.test_toJSON()) shouldEqual df2Seq(snow.test_toJSON())
  }

  "collectAsList" should "match Spark collectAsList" in {
    sparkList2Seq(spark.test_collectAsList()) shouldEqual snowList2Seq(snow.test_collectAsList())
  }
}
