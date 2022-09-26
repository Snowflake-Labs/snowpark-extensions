package com.snowflake.snowpark_extensions.implicits

//Testing packages
import com.snowflake.snowpark_extensions.implicits.Snowpark._
import com.snowflake.snowpark_extensions.implicits.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers}

class SessionExtensionsTest extends FlatSpec with Matchers {
  behavior of "SessionExtensions class"

  "time" should "match spark time" in {
    SessionExtensionsSpark.test_time() shouldEqual SessionExtensionsSnowpark.test_time()
  }

  "catalog" should "match spark catalog" in {
    df2Seq(SessionExtensionsSpark.test_catalog_methods()) shouldEqual df2Seq(SessionExtensionsSnowpark.test_catalog_methods())
  }

  "conf" should "match spark conf" in {
    SessionExtensionsSpark.test_conf().toSeq shouldEqual SessionExtensionsSnowpark.test_conf().toSeq
  }

  "execute" should "match hive execute" in {
    df2Seq(SessionExtensionsSpark.test_execute()) shouldEqual df2Seq(SessionExtensionsSnowpark.test_execute())
  }

}
