package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.implicits.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers}

class ColumnExtensionsTest extends FlatSpec with Matchers {
  behavior of "ColumnExtensions class"

  "isin" should "match spark isin" in {
    // -------------------------------------
    // |"COL1"  |"COL2"  |"COL3"  |"COL4"  |
    // -------------------------------------
    // |2       |2.1     |two     |c       |
    // -------------------------------------
    Seq(Seq(2,2.1,"two","c")) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isin())
  }

  "as(symbol)" should "match spark as(symbol)" in {
    // ----------------------------
    // |"MYSYMBOL"                |
    // ----------------------------
    // |NULL                      |
    // |two                       |
    // |two hundred thirty seven  |
    // ----------------------------
    Seq(Seq(null),Seq("two"),Seq("two hundred thirty seven")) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_as_symbol())
  }

  "isNaN" should "match spark isNaN" in {
    // +-----------+-----------+
    // |isnan(col1)|isnan(col2)|
    // +-----------+-----------+
    // |      false|      false|
    // |       true|      false|
    // +-----------+-----------+
    Seq(Seq(false,false),Seq(true,false)) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNaN())
  }

  "isNull" should "match spark isNull" in {
    // +--------------+--------------+
    // |(col1 IS NULL)|(col3 IS NULL)|
    // +--------------+--------------+
    // |         false|          true|
    // |         false|         false|
    // |         false|         false|
    // +--------------+--------------+    
    Seq(Seq(false,true),Seq(false,false),Seq(false,false)) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNull())
  }

  "isNotNull" should "match spark isNotNull" in {
    // +------------------+------------------+
    // |(col1 IS NOT NULL)|(col3 IS NOT NULL)|
    // +------------------+------------------+
    // |              true|             false|
    // |              true|              true|
    // |              true|              true|
    // +------------------+------------------+ 
    Seq(Seq(true,false),Seq(true,true),Seq(true,true)) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_isNotNull())
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
// |int_to_str|str_to_bool|int_to_bool|str_to_byte|str_to_short|str_to_int|str_to_long|str_to_float|str_to_double|str_to_decimal|str_to_date|   str_to_timestamp|
// +----------+-----------+-----------+-----------+------------+----------+-----------+------------+-------------+--------------+-----------+-------------------+
// |         1|       true|       true|        123|         123|       123|        123|         2.1|          2.1|             2| 2021-05-15|2021-05-15 06:54:34|
// |         0|      false|      false|         15|          15|        15|         15|         5.4|          5.4|             5| 2021-05-15|2021-05-15 00:00:00|
// |         0|       null|      false|          0|           0|         0|          0|         0.0|          0.0|             0|       null|               null|
// +----------+-----------+-----------+-----------+------------+----------+-----------+------------+-------------+--------------+-----------+-------------------+
    Seq(
Seq( "1", true,  true,123,123,123,123,2.1,2.1,2, java.sql.Date.valueOf("2021-05-15"),java.sql.Timestamp.valueOf("2021-05-15 06:54:34")),
Seq( "0",false, false, 15, 15, 15, 15,5.4,5.4,5, java.sql.Date.valueOf("2021-05-15"),java.sql.Timestamp.valueOf("2021-05-15 00:00:00")),
Seq( "0", null, false,  0,  0,  0,  0,0.0,0.0,0,       null,    null))   shouldEqual df2Seq(ColumnExtensionsSnowpark.test_cast())
  }

  "eqNullSafe" should "match eqNullSafe" in {
     df2Seq(ColumnExtensionsSpark.test_eqNullSafe()) shouldEqual df2Seq(ColumnExtensionsSnowpark.test_eqNullSafe())
  }

}
