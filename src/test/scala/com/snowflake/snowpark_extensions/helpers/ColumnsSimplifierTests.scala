package com.snowflake.snowpark_extensions.helpers

//Testing packages
import com.snowflake.snowpark_extensions.helpers.Snowpark._
import com.snowflake.snowpark_extensions.helpers.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers}

class ColumnsSimplifierTests extends FlatSpec with Matchers{
  behavior of "ColumnsSimplifier class"

  "continous withColums" should "match spark continous withColums" in {
    df2Seq(ColumnsSimplifierSpark.test_columns_simplifier()) shouldEqual df2Seq(ColumnsSimplifierSnowpark.test_columns_simplifier())
  }

}