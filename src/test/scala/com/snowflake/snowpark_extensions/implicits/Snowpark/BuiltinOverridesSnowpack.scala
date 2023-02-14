package com.snowflake.snowpark_extensions.implicits.Snowpark

import com.snowflake.snowpark_extensions.testutils.DataFrameCreator.{data_for_float_column}
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import com.snowflake.snowpark_extensions.helpers.BuiltinOverrides.regexp_split

object BuiltinOverridesSnowpack {

  // Init
  val session = SessionInitializer.snow
  val df2 = session.createDataFrame(DataFrameCreator.data_for_float).toDF { data_for_float_column }
  def test_split() = df2.withColumn("new", regexp_split(df2("COL3"), "t")(session))

}