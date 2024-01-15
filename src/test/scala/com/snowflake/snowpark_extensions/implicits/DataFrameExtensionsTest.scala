package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.testutils.Serializer.{df2Seq, snowList2Seq}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import com.snowflake.snowpark_extensions.implicits.Snowpark.{DataFrameExtensionsSnowpark => snow}


class DataFrameExtensionsTest extends AnyFlatSpec with should.Matchers {
  behavior of "DataFrameExtensions class"

  "withColumnRenamed" should "match spark withColumnRenamed" in {
    // +---+-----+--------------------+----+
    // |  a| col2|                col3|col4|
    // +---+-----+--------------------+----+
    // |  1|  1.1|                null|   c|
    // |  2|  2.1|                 two|   c|
    // |237|237.1|two hundred thirty seven|   g|
    // +---+-----+--------------------+----+
    Seq(Seq(1,1.1,null,"c"),Seq(2,2.1,"two","c"),Seq(237,237.1,"two hundred thirty seven","g")) shouldEqual df2Seq(snow.test_withColumnRenamed())
  }

  "filter with SQL expression" should "match Spark filter with SQL expression" in {
    // +----+-----+------------------------+----+
    // |col1|col2 |col3                    |col4|
    // +----+-----+------------------------+----+
    // |2   |2.1  |two                     |c   |
    // |237 |237.1|two hundred thirty seven|g   |
    // +----+-----+------------------------+----+ 
    Seq(Seq(2,2.1,"two","c"),Seq(237,237.1,"two hundred thirty seven","g")) shouldEqual df2Seq(snow.test_filterSQLExpression())
  }

  "filter with complex SQL expression" should "match Spark filter with complex SQL expression" in {
    Seq() shouldEqual df2Seq(snow.test_filterSQLExpressionComplex())
  }

  "custom selectExpr" should "match Spark selectExpr" in {
    // +------------+---------+
    // |(col2 + 8.8)|col1_text|
    // +------------+---------+
    // |9.9         |One      |
    // |10.9        |Two      |
    // |245.9       |Two      |
    // +------------+---------+
    Seq(Seq(9.9,"One"),Seq(10.9,"Two"),Seq(245.9,"Two")) shouldEqual df2Seq(snow.test_selectExpr())
  }

  "custom selectExpr with Cast" should "match Spark selectExpr with Cast" in {
    // +----------+-------------------+
    // |date_only |date_time          |
    // +----------+-------------------+
    // |null      |null               |
    // |2021-05-15|2021-05-15 06:54:34|
    // |2022-08-01|2022-08-01 14:12:16|
    // +----------+-------------------+
    Seq(
    Seq(null,null),
    Seq(java.sql.Date.valueOf("2021-05-15"), java.sql.Timestamp.valueOf("2021-05-15 06:54:34")),
    Seq(java.sql.Date.valueOf("2022-08-01"), java.sql.Timestamp.valueOf("2022-08-01 14:12:16"))) shouldEqual df2Seq(snow.test_selectExprWithCast())
  }

  "head(n)" should "match Spark head(n)" in {
    // +----+-----+------------------------+----+
    // |col1|col2 |col3                    |col4|
    // +----+-----+------------------------+----+
    // |1   |1.1  |null                    |c   |
    // |2   |2.1  |two                     |c   |
    // +----+-----+------------------------+----+ 
    Seq(Seq(1,1.1,null,"c"),Seq(2,2.1,"two","c")) shouldEqual snow.test_headN()
  }

  "head" should "match Spark head" in {
    // +----+-----+------------------------+----+
    // |col1|col2 |col3                    |col4|
    // +----+-----+------------------------+----+
    // |1   |1.1  |null                    |c   |
    // +----+-----+------------------------+----+     
    Array(1,1.1,null,"c") shouldEqual snow.test_head()
  }

  "simple transform" should "match Spark simple transform" in {
    // +----+-----+------------------------+----+----+
    // |col1|col2 |col3                    |col4|test|
    // +----+-----+------------------------+----+----+
    // |2   |2.1  |two                     |c   |7   |
    // |237 |237.1|two hundred thirty seven|g   |242 |
    // +----+-----+------------------------+----+----+ 
    Seq(Seq(2,2.1,"two","c",7),Seq(237,237.1,"two hundred thirty seven","g",242)) shouldEqual df2Seq(snow.test_transformSimple())
  }

  "chained transform" should "match Spark chained transform" in {
    // +----+-----+------------------------+----+----+
    // |col1|col2 |col3                    |col4|test|
    // +----+-----+------------------------+----+----+
    // |2   |2.1  |two                     |c   |7   |
    // |237 |237.1|two hundred thirty seven|g   |242 |
    // +----+-----+------------------------+----+----+    
   Seq(Seq(2,2.1,"two","c",7),Seq(237,237.1,"two hundred thirty seven","g",242)) shouldEqual df2Seq(snow.test_transformChained())
  }

  "transform with params" should "match Spark transform with params" in {
    // +----+-----+------------------------+----+----+
    // |col1|col2 |col3                    |col4|test|
    // +----+-----+------------------------+----+----+
    // |2   |2.1  |two                     |c   |7   |
    // |237 |237.1|two hundred thirty seven|g   |242 |
    // +----+-----+------------------------+----+----+    
    Seq(Seq(2,2.1,"two","c",7),Seq(237,237.1,"two hundred thirty seven","g",242)) shouldEqual df2Seq(snow.test_transformWithParams())
  }

  "collectAsList" should "match Spark collectAsList" in {
    Seq(Seq(1,1.1,null,"c"), Seq(2,2.1,"two","c"), Seq(237,237.1,"two hundred thirty seven","g")) shouldEqual snowList2Seq(snow.test_collectAsList())
  }
}
