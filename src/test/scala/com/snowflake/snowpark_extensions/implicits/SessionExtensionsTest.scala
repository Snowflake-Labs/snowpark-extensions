package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SessionExtensionsTest extends AnyFlatSpec with should.Matchers {

  // "time" should "match spark time" in {
  //   val str = SessionExtensionsSnowpark.test_time()
  //   str should startWith ("Time taken")
  // }

  "catalog" should "match spark catalog" in {
    // +----+--------+
    // |name|nullable|
    // +----+--------+
    // |col1|true    |
    // |col2|true    |
    // |a   |true    |
    // |b   |true    |
    // +----+--------+
    Seq(Seq("col1",true),Seq("col2",true),Seq("a",true),Seq("b",true)) shouldEqual df2Seq(SessionExtensionsSnowpark.test_catalog_methods())
  }

  // "conf" should "match spark conf" in {
  //   SessionExtensionsSpark.test_conf().toSeq shouldEqual SessionExtensionsSnowpark.test_conf().toSeq
  // }

  // "execute" should "match hive execute" in {
  //   df2Seq(SessionExtensionsSpark.test_execute()) shouldEqual df2Seq(SessionExtensionsSnowpark.test_execute())
  // }

}
