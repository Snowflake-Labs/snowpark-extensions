package com.snowflake.snowpark_extensions.helpers

//Testing packages
import com.snowflake.snowpark_extensions.helpers.Snowpark._
import com.snowflake.snowpark_extensions.helpers.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers}

class SnowflakeBuiltinTests extends FlatSpec with Matchers {
  behavior of "SnowflakeBuiltin class"

  "size" should "match spark size" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_size()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_size())
  }

  "expr" should "match spark expr" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_expr()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_expr())
  }

  "column" should "match spark column" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_column()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_column())
  }

  "array" should "match spark array" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_array()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_array())
  }

  "concat" should "match spark concat" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_concat()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_concat())
  }

  "shiftleft" should "match Spark shiftleft" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_shiftleft()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_shiftleft())
  }

  "shiftright" should "match Spark shiftright" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_shiftright()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_shiftright())
  }

  "hex" should "match Spark hex" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_hex()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_hex())
  }

  "unhex" should "match Spark decode(unhex)" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_unhex()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_unhex())
  }

  "json_tuple" should "match Spark json_tuple" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_json_tuple()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_json_tuple())
  }

  "cbrt" should "match Spark cbrt" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_cbrt()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_cbrt())
  }

  "from_json" should "match Spark from_json" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_from_json()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_from_json())
  }

  "from_json all data types" should "match Spark from_json with all data types" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_from_jsonAllDatatypes()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_from_jsonAllDatatypes())
  }

  "date_sub" should "match Spark date_sub" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_date_sub()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_date_sub())
  }

  "bround" should "match Spark bround" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_bround()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_bround())
  }

  "regexp_extract" should "match Spark regexp_extract" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_regexp_extract()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_regexp_extract())
  }

  "signum" should "match Spark signum" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_signum()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_signum())
  }

  "nanvl" should "match Spark nanvl" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_nanvl()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_nanvl())
  }

   "log10" should "match Spark log10" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_log10()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_log10())
  }


   "log1p" should "match Spark log1p" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_log1p()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_log1p())
  }

  "base64" should "match Spark base64" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_base64()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_base64())
  }

  "unbase64" should "match Spark unbase64" ignore {
    val spark = df2Seq(SnowflakeBuiltinSpark.test_unbase64()) 
    val snowpark = df2Seq(SnowflakeBuiltinSnowpark.test_unbase64())
    spark(0)(0) shouldEqual snowpark(0)(0)
    spark(0)(1) shouldEqual snowpark(0)(1)
    spark(0)(2) shouldEqual snowpark(0)(2)
  }

  "locate" should "match Spark locate" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_locate()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_locate())
  }

  "format_string" should "match Spark format_string" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_format_string()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_format_string())
  }

  "arrayString" should "match spark arrayString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_arrayString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_arrayString())
  }

  "collectList" should "match spark collectList" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_collect_list()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_collect_list())
  }

  "collectListString" should "match spark collectListString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_collect_list_string()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_collect_list_string())
  }

  "lastString" should "match spark lastString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_last_string()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_last_string())
  }

  "last" should "match spark last" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_last()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_last())
  }

  "reverse" should "match spark reverse" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_reverse()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_reverse())
  }

  "isnull" should "match spark isnull" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_isnull()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_isnull())

  }

  "lastIgnoreNulls" should "match spark lastIgnoreNulls" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_last_ignoreNulls()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_last_ignoreNulls())
  }

  "lastIgnoreNullsString" should "match spark lastIgnoreNullsString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_last_ignoreNullsString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_last_ignoreNullsString())
  }

  "firstString" should "match spark firstString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_first_string()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_first_string())
  }

  "first" should "match spark first" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_first()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_first())
  }

  "firstIgnoreNulls" should "match spark firstIgnoreNulls" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_first_ignoreNulls()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_first_ignoreNulls())
  }

  "firstIgnoreNullsString" should "match spark firstIgnoreNullsString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_first_ignoreNullsString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_first_ignoreNullsString())
  }

  "lag" should "match spark lag" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_lag()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_lag())
  }

  "lagString" should "match spark lagString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_lagString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_lagString())
  }

  "lagDefault" should "match spark lagDefault" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_lagDefault()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_lagDefault())
  }

  "lagDefaultString" should "match spark lagDefaultString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_lagDefaultString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_lagDefaultString())
  }

  "lead" should "match spark lead" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_lead()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_lead())
  }

  "leadString" should "match spark leadString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_leadString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_leadString())
  }

  "leadDefault" should "match spark leadDefault" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_leadDefault()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_leadDefault())
  }

  "leadDefaultString" should "match spark leadDefaultString" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_leadDefaultString()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_leadDefaultString())
  }

  "conv" should "match spark conv" ignore {
    df2Seq(SnowflakeBuiltinSpark.test_conv()) shouldEqual df2Seq(SnowflakeBuiltinSnowpark.test_conv())
  }
}