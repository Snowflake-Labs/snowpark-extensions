package com.snowflake.snowpark_extensions.implicits.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lit,struct,when}
import net.snowflake.client.jdbc.internal.apache.tika.metadata.Metadata
import org.apache.spark.sql

object CaseExprExtensionsSpark {

  // Init
  val session = SessionInitializer.spark
  val df2 = session.createDataFrame(DataFrameCreator.data_for_float).toDF(DataFrameCreator.data_for_float_column:_*)

  //Function: otherwise(value: Any): Column
  def test_otherwise() : DataFrame = {
    df2.withColumn("colNew", when(col("col3")==="5",lit("1")).otherwise("0"))
  }

}