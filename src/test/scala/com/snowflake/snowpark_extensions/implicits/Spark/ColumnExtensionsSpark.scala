package com.snowflake.snowpark_extensions.implicits.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lit,struct}
import net.snowflake.client.jdbc.internal.apache.tika.metadata.Metadata
import org.apache.spark.sql

object ColumnExtensionsSpark {

  // Init
  val session = SessionInitializer.spark
  val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column:_*)
  val df2 = session.createDataFrame(DataFrameCreator.data_for_float).toDF(DataFrameCreator.data_for_float_column:_*)
  val df3 = session.createDataFrame(DataFrameCreator.data_for_array).toDF(DataFrameCreator.data_for_array_column:_*)
  val df4 = session.createDataFrame(DataFrameCreator.data_for_cast).toDF(DataFrameCreator.data_for_cast_column:_*)

  // Process

  //Function isin(list: Any*): Column
  def test_isin(): DataFrame ={
    df.filter(col("col3").isin("two"))
  }

  // Function: as(alias: Symbol): Column
  def test_as_symbol(): DataFrame ={
    df.select(col("col3").as('mySymbol))
  }

  //Function: isNaN: Column
  def test_isNaN(): DataFrame = {  
    df2.select(col("col1").isNaN, col("col2").isNaN)
  }

  //Function: isNull: Column
  def test_isNull(): DataFrame = {  
    df.select(col("col1").isNull, col("col3").isNull)
  }
  
  //Function: isNotNull: Column
  def test_isNotNull(): DataFrame = {  
    df.select(col("col1").isNotNull, col("col3").isNotNull)
  }

  //Function: substr(startPos: Int, len: Int): Column
  def test_substrByInts(): DataFrame = {  
    df2.select(col("col3").substr(0,2), col("col3").substr(1,2), col("col3").substr(2,7))
  }

  //Function: substr(startPos: Column, len: Column): Column
  def test_substrByCols(): DataFrame = {  
    df2.select(col("col3").substr(lit(0),lit(2)), col("col3").substr(lit(1),lit(2)), col("col3").substr(lit(2),lit(7)))
  }

  //Function: startsWith(literal: String): Column
  def test_startsWith() : DataFrame =
    df.select(col("col3").startsWith("a"), col("col3").startsWith("t"))

  //Function: notEqual(other: Any): Column
  def test_notEqual() : DataFrame =
    df.select(col("col1").notEqual(col("col2")), col("col3").notEqual(col("col3")))

  //Function: rlike(literal: String): Column
  def test_rlike() : DataFrame =
    df2.select(col("col3").rlike("test\\d{3}"))

  //Function: bitwiseAND(other: Any): Column
  def test_bitwiseAND() : DataFrame =
    df2.select(col("col2").bitwiseAND(col("col4")))

  //Function: bitwiseOR(other: Any): Column
  def test_bitwiseOR() : DataFrame =
    df2.select(col("col2").bitwiseOR(col("col4")))

  //Function: bitwiseXOR(other: Any): Column
  def test_bitwiseXOR() : DataFrame =
    df2.select(col("col2").bitwiseXOR(col("col4")))

  //Function getItem(key: Any): Column
  def test_getItem(): DataFrame =
    df3.select(col("col1").getItem(0).as("pos0"), col("col1").getItem(1).as("pos1"), col("col1").getItem(2).as("pos2"))

  //Function getField(fieldName: String): Column
  def test_getField(): DataFrame = {  
    df.select(struct(col("col2").as("Col 2"), col("col3").as("Col 3")).getField("Col 3"))
  }

  //Function contains(other: Any): Column
  def test_contains(): DataFrame = {  
    df2.select(col("col3").contains("test"), col("col3").contains("123"))
  }

  //Function cast(to: String): Column
  def test_cast(): DataFrame = {  
    df4.select(
              col("col2").cast("string").as("int_to_str"),
              col("col1").cast("boolean").as("str_to_bool"), 
              col("col2").cast("boolean").as("int_to_bool"), 
              col("col3").cast("byte").as("str_to_byte"),
              col("col3").cast("short").as("str_to_short"),
              col("col3").cast("int").as("str_to_int"), 
              col("col3").cast("long").as("str_to_long"),
              col("col4").cast("float").as("str_to_float"),
              col("col4").cast("double").as("str_to_double"),
              col("col4").cast("decimal").as("str_to_decimal"),
              col("col5").cast("date").as("str_to_date"),
              col("col5").cast("timestamp").as("str_to_timestamp"))
  }

  def test_eqNullSafe(): DataFrame = { 
    df.select(col("col3").eqNullSafe("two"),
              col("col3").eqNullSafe(null),
              col("col3") <=> null)
  }

  //Main
  def main(args: Array[String]): Unit = {
    // Spark testing main{

    var a = test_eqNullSafe()
    a.show()
  }
}