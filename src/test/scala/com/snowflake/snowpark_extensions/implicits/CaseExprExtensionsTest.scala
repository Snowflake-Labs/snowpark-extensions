package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers, Ignore}

class CaseExprExtensionsTest extends FlatSpec with Matchers {
behavior of "CaseExprExtensions class"

  "otherwise" should "match spark otherwise" in {
    val casSnowpark = CaseExprExtensionsSnowpark.test_otherwise()
    var colNewSpark = casSnowpark.collect()
    colNewSpark.foreach(r=> assert(r(4) == "0"))
  }

}