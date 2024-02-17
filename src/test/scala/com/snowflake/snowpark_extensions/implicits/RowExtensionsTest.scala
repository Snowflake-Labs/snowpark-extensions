package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._

class RowExtensionsTest extends AnyFlatSpec with should.Matchers {

  "getAs" should "match spark getAs" in {
    var df = DataFrameExtensionsSnowpark.session.sql("select 1 as a, 2 as b, 'Hi' as c")
    var rows = df.collectExt()
    assert(0 == rows(0).fieldIndex("a"))
    assert(1 == rows(0).fieldIndex("b"))
    assert(2 == rows(0).fieldIndex("c"))
    assert(1 == rows(0).getAs[Int](0))
    assert(1 == rows(0).getAs[Int]("a"))
    assert(2 == rows(0).getAs[Int](1))
    assert(2 == rows(0).getAs[Int]("b"))
    assert("Hi" == rows(0).getAs[String](2))
    assert("Hi" == rows(0).getAs[String]("c"))
  }

}