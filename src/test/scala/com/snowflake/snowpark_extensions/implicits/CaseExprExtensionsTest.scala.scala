package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.implicits.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers, Ignore}

class CaseExprExtensionsTest extends FlatSpec with Matchers {
behavior of "CaseExprExtensions class"

  "otherwise" should "match spark otherwise" in {
    var casSpark = CaseExprExtensionsSpark.test_otherwise()
    val casSnowpark = CaseExprExtensionsSnowpark.test_otherwise()
    var colNewSpark = casSpark.collect()
    colNewSpark.foreach(r=> assert(r(4) == "0"))
    var colNew = casSpark.collect()
    colNew.foreach(r=> assert(r(4) == "0"))
  }

}