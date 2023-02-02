package com.snowflake.snowpark_extensions.implicits.Snowpark

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.functions.{col, lit, object_construct, get, when}
import com.snowflake.snowpark.types.StringType
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.testutils.DataFrameCreator.{data_for_general_column, data_for_float_column, data_for_array_column, data_for_cast_column}
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}


object CaseExprExtensionsSnowpark {

  // Init
  val session = SessionInitializer.snow
  val df2 = session.createDataFrame(DataFrameCreator.data_for_float).toDF(data_for_float_column)

  //Function: otherwise(value: Any): Column
  def test_otherwise() : DataFrame = {
    df2.withColumn("colNew", when(col("COL3")==="5",lit("1")).otherwise("0"))//.show()
  }

}