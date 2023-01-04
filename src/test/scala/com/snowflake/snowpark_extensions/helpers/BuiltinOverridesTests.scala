package com.snowflake.snowpark_extensions.helpers

//Testing packages
import com.snowflake.snowpark_extensions.helpers.Snowpark._
import com.snowflake.snowpark_extensions.helpers.Spark._
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import org.scalatest.{FlatSpec, Matchers, Ignore}

class BuiltinOverridesTests extends FlatSpec with Matchers{
  behavior of "BuiltinOverrides class"

  "concat" should "match spark concat" ignore {
    df2Seq(BuiltinOverridesSpark.test_concat()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_concat())
  }

  "concat_ws" should "match spark concat_ws" ignore {
    df2Seq(BuiltinOverridesSpark.test_concat_ws()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_concat_ws())
  }

  "avg" should "match spark avg" ignore {
    df2Seq(BuiltinOverridesSpark.test_avg()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_avg())
  }

  "lead" should "match spark lead" ignore {
    df2Seq(BuiltinOverridesSpark.test_lead()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_lead())
  }

  "leadString" should "match spark leadString" ignore {
    df2Seq(BuiltinOverridesSpark.test_leadString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_leadString())
  }

  "leadDefault" should "match spark leadDefault" ignore {
    df2Seq(BuiltinOverridesSpark.test_leadDefault()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_leadDefault())
  }

  "leadDefaultString" should "match spark leadDefaultString" ignore {
    df2Seq(BuiltinOverridesSpark.test_leadDefaultString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_leadDefaultString())
  }

  "lag" should "match spark lag" ignore {
    df2Seq(BuiltinOverridesSpark.test_lag()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_lag())
  }

  "lagString" should "match spark lagString" ignore {
    df2Seq(BuiltinOverridesSpark.test_lagString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_lagString())
  }

  "lagDefault" should "match spark lagDefault" ignore {
    df2Seq(BuiltinOverridesSpark.test_lagDefault()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_lagDefault())
  }

  "lagDefaultString" should "match spark lagDefaultString" ignore {
    df2Seq(BuiltinOverridesSpark.test_lagDefaultString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_lagDefaultString())
  }

  "approx_count_distinct" should "match spark approx_count_distinct" ignore {
    df2Seq(BuiltinOverridesSpark.test_approx_count_distinctString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_approx_count_distinctString())
  }

  "approx_count_distinctString" should "match spark approx_count_distinctString" ignore {
    df2Seq(BuiltinOverridesSpark.test_approx_count_distinctString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_approx_count_distinctString())
  }

  "degrees" should "match spark degrees" ignore {
    df2Seq(BuiltinOverridesSpark.test_degrees()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_degrees())
  }

  "degreesString" should "match spark degreesString" ignore {
    df2Seq(BuiltinOverridesSpark.test_degreesString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_degreesString())
  }

  "radians" should "match spark radians" in {
    df2Seq(BuiltinOverridesSpark.test_radians()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_radians())
  }

  "radiansString" should "match spark radiansString" in {
    df2Seq(BuiltinOverridesSpark.test_radiansString()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_radiansString())
  }

  "ntile" should "match spark ntile" ignore {
    df2Seq(BuiltinOverridesSpark.test_ntile()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_ntile())
  }

  "atan2" should "match spark atan2" in {
    df2Seq(BuiltinOverridesSpark.test_atan2()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2())
  }

  "atan2strCol" should "match spark atan2strCol" in {
    df2Seq(BuiltinOverridesSpark.test_atan2strCol()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2strCol())
  }

  "atan2colStr" should "match spark atan2colStr" in {
    df2Seq(BuiltinOverridesSpark.test_atan2colStr()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2colStr())
  }

  "atan2str" should "match spark atan2str" in {
    df2Seq(BuiltinOverridesSpark.test_atan2str()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2str())
  }

  "atan2colDbl" should "match spark atan2colDbl" in {
    df2Seq(BuiltinOverridesSpark.test_atan2colDbl()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2colDbl())
  }

  "atan2strDbl" should "match spark atan2strDbl" in {
    df2Seq(BuiltinOverridesSpark.test_atan2strDbl()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2strDbl())
  }

  "atan2dblCol" should "match spark atan2dblCol" in {
    df2Seq(BuiltinOverridesSpark.test_atan2dblCol()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2dblCol())
  }

  "atan2dblStr" should "match spark atan2dblStr" in {
    df2Seq(BuiltinOverridesSpark.test_atan2dblStr()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_atan2dblStr())
  }

  "acos" should "match spark acos" in {
    df2Seq(BuiltinOverridesSpark.test_acos()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_acos())
  }

  "trim" should "match spark trim" in {
    df2Seq(BuiltinOverridesSpark.test_trim()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_trim())
  }

  "rtrim" should "match spark rtrim" in {
    df2Seq(BuiltinOverridesSpark.test_rtrim()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_rtrim())
  }

  "ltrim" should "match spark trim" in {
    df2Seq(BuiltinOverridesSpark.test_trim()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_ltrim())
  }

  "split" should "match spark split" in {
    val sparkSplit = BuiltinOverridesSpark.test_split()
    val collectSparkSplit = sparkSplit.collect()
    collectSparkSplit.foreach(r=> assert(r(0) === Array("0", "1"))) 
  }
  
  "acosStr" should "match spark acosStr" in {
    df2Seq(BuiltinOverridesSpark.test_acosStr()) shouldEqual df2Seq(BuiltinOverridesSnowpark.test_acosStr())
  }

  "round" should "match spark round" in {

    val doubleValue = 22.0f
    val sparkRound = BuiltinOverridesSpark.test_round()
    val collectsSarkRound = sparkRound.collect()
    collectsSarkRound.foreach(r=> assert(r(0) === doubleValue)) 
    val snowparkRound = BuiltinOverridesSnowpark.test_round()
    val collectsSnowparkRound = sparkRound.collect()
    collectsSnowparkRound.foreach(r=> assert(r(0) === doubleValue)) 
  }

    "repeat" should "match spark repeat" in {

    val doubleValue = "22.132122.1321"
    val sparkRepeat = BuiltinOverridesSpark.test_repeat()
    val collectsSparkRepeat = sparkRepeat.collect()
    collectsSparkRepeat.foreach(r=> assert(r(0) === doubleValue)) 
    val snowparkRepeat = BuiltinOverridesSnowpark.test_repeat()
    val collectsSnowparkRepeat = snowparkRepeat.collect()
    collectsSnowparkRepeat.foreach(r=> assert(r(0) === doubleValue)) 
  }

      "translate" should "match spark translate" in {

    val doubleValue = "11.1311"
    val sparkTranslate = BuiltinOverridesSpark.test_translate()
    sparkTranslate.show()
    val collectsSparkTranslate = sparkTranslate.collect()
    collectsSparkTranslate.foreach(r=> assert(r(0) === doubleValue)) 
    val snowparkTranslate = BuiltinOverridesSnowpark.test_translate()
    snowparkTranslate.show()
    val collectsSnowparkTranslate = snowparkTranslate.collect()
    collectsSnowparkTranslate.foreach(r=> assert(r(0) === doubleValue)) 
  }

}