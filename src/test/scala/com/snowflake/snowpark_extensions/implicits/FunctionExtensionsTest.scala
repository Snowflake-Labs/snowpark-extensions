
package com.snowflake.snowpark_extensions.implicits

import com.snowflake.snowpark_extensions.Extensions.functions._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import com.snowflake.snowpark_extensions.testutils.SessionInitializer
import org.scalatest.{FlatSpec, Matchers}
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.functions.{col}
import com.snowflake.snowpark.types._

class FunctionExtensionsTest extends FlatSpec with Matchers {
  behavior of "FunctionExtenions class"

  "when expression" should "compile without errors" in {
    when(col("a") > 1,"literal1").otherwise("literal2")
  }

  "substring_index" should "match spark substring_index" in {
    // df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    // df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    // [Row(s='a.b')]
    // df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    // [Row(s='b.c.d')]
    val session = SessionInitializer.snow
    // Create an array of Row objects containing data.
    val data = Seq(("a.b.c.d"))
    // Define the schema for the columns in the DataFrame.
    import com.snowflake.snowpark_extensions.Extensions.functions._
    // Create the DataFrame.
    val df = session.createDataFrame(data).toDF(Seq("s"))
    Seq(Seq("a.b"))   shouldEqual df2Seq(df.select(substring_index(df("s"),".",2)))
  }

  "substring_index negative index" should "match spark substring_index" in {
    // df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    // df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    // [Row(s='a.b')]
    // df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    // [Row(s='b.c.d')]
    val session = SessionInitializer.snow
    // Create an array of Row objects containing data.
    val data = Seq(("a.b.c.d"))
    // Define the schema for the columns in the DataFrame.
    import com.snowflake.snowpark_extensions.Extensions.functions._
    // Create the DataFrame.
    val df = session.createDataFrame(data).toDF(Seq("s"))
    Seq(Seq("b.c.d")) shouldEqual df2Seq(df.select(substring_index(df("s"),".",-3)))
  }

}