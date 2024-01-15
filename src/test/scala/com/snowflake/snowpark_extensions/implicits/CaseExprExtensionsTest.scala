package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class CaseExprExtensionsTest extends AnyFlatSpec with should.Matchers {

  "otherwise" should "match spark otherwise" in {
    val casSnowpark = CaseExprExtensionsSnowpark.test_otherwise()
    var colNewSpark = casSnowpark.collect()
    colNewSpark.foreach(r=> assert(r(4) == "0"))
  }

}