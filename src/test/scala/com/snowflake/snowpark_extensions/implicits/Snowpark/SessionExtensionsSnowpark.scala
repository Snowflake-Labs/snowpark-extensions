package com.snowflake.snowpark_extensions.implicits.Snowpark
import com.snowflake.snowpark.Session
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark_extensions.testutils.DataFrameCreator.data_for_general_column
import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import com.snowflake.snowpark_extensions.Extensions._
object SessionExtensionsSnowpark {

  // Init
  val session = SessionInitializer.snow
  val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(data_for_general_column)

//#region Process

  def test_builder_methods() : Unit = {
    val session = Session.builder.from_snowsql().create
    session.sql("select 'hi world'").show()
  }

  def test_time() : String = {
    val baos = new java.io.ByteArrayOutputStream
    val ps = new java.io.PrintStream(baos)
    System.setOut(ps)
    session.time( () => df.count)
    new String(baos.toByteArray)
  }

def test_catalog_methods() : DataFrame = {
     val name = "TestDB"
     val tableName = "TestTable"

     val catalog = session.catalog
     //catalog.setCurrentDatabase(name)
     session.sql("""CREATE OR REPLACE TABLE TestTable("col1" int, "col2" string, "a" int, "b" string)""").collect()
     val columns = catalog.listColumns(tableName)
     val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column)
     df.createOrReplaceTempView("view1")
     catalog.dropTempView("view1")
     columns.drop("description").drop("dataType").drop("isPartition").drop("isBucket")
  }

  def test_conf(): Array[Any] = {
    session.conf.set("spark.executor.coresX",4)
    val cores_val = session.conf.get("spark.executor.coresX")
    session.conf.set("someProp",true)
    val someProp = session.conf.get("someProp")
    Array(cores_val, someProp)
  }

  def test_execute(): DataFrame ={
    df.createOrReplaceTempView("test_execute")
    session.execute("select * from test_execute")
  }
//#end region

  //Main
  def main(args: Array[String]): Unit = {
    test_builder_methods()
  }

}
