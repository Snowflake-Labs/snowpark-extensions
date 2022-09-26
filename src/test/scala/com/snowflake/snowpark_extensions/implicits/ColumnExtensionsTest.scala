package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.implicits.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers}

class ColumnExtensionsTest extends FlatSpec with Matchers {
  behavior of "ColumnExtensions class"

  "isin" should "match spark isin" in {
    df2Seq(ColumnExtensionsSpark.test_isin()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isin())
  }

  "as(symbol)" should "match spark as(symbol)" in {
    df2Seq(ColumnExtensionsSpark.test_as_symbol()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_as_symbol())
  }

  "isNaN" should "match spark isNaN" in {
    df2Seq(ColumnExtensionsSpark.test_isNaN()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNaN())
  }

  "isNull" should "match spark isNull" in {
    df2Seq(ColumnExtensionsSpark.test_isNull()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNull())
  }

  "isNotNull" should "match spark isNotNull" in {
    df2Seq(ColumnExtensionsSpark.test_isNotNull()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNotNull())
  }

  "substr(int, int)" should "match spark (int, int)" in {
    df2Seq(ColumnExtensionsSpark.test_substrByInts()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_substrByInts())
  }

  "substr(col, col)" should "match spark (col, col)" in {
    df2Seq(ColumnExtensionsSpark.test_substrByCols()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_substrByCols())
  }

  "startsWith" should "match startsWith" in {
    df2Seq(ColumnExtensionsSpark.test_startsWith()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_startsWith())
  }

  "notEqual" should "match notEqual" in {
    df2Seq(ColumnExtensionsSpark.test_notEqual()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_notEqual())
  }

  "rlike" should "match rlike" in {
     df2Seq(ColumnExtensionsSpark.test_rlike()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_rlike())
  }

  "bitwiseAND" should "match bitwiseAND" in {
     df2Seq(ColumnExtensionsSpark.test_bitwiseAND()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_bitwiseAND())
  }

  "bitwiseOR" should "match bitwiseOR" in {
     df2Seq(ColumnExtensionsSpark.test_bitwiseOR()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_bitwiseOR())
  }

  "bitwiseXOR" should "match bitwiseXOR" in {
     df2Seq(ColumnExtensionsSpark.test_bitwiseXOR()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_bitwiseXOR())
  }

  "getItem" should "match getItem" in {
     df2Seq(ColumnExtensionsSpark.test_getItem()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_getItem())
  }

  "getField" should "match getField" in {
     df2Seq(ColumnExtensionsSpark.test_getField()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_getField())
  }

  "contains" should "match contains" in {
     df2Seq(ColumnExtensionsSpark.test_contains()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_contains())
  }

  "cast" should "match cast" in {
     df2Seq(ColumnExtensionsSpark.test_cast()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_cast())
  }

  "eqNullSafe" should "match eqNullSafe" in {
     df2Seq(ColumnExtensionsSpark.test_eqNullSafe()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_eqNullSafe())
  }

}
