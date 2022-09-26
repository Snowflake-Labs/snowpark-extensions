package com.snowflake.snowpark_extensions.implicits.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import java.net.URI
import org.apache.spark.util.ShutdownHookManager
import com.snowflake.snowpark_extensions.testutils.Utils
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.TableIdentifier
import com.snowflake.snowpark.types.StructField
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.types.StringType
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq

object SessionExtensionsSpark {

  // Init
  val session = SessionInitializer.spark
  val df = session
    .createDataFrame(DataFrameCreator.data_for_general)
    .toDF(DataFrameCreator.data_for_general_column: _*)

//#region Process


  def test_time() : String = {
    val baos = new java.io.ByteArrayOutputStream
    val ps = new java.io.PrintStream(baos)
    System.setOut(ps)
    session.time( () => df.count)
    new String(baos.toByteArray).replaceAll("\\d+","0")
  }
  def test_execute(): DataFrame ={
    df.createOrReplaceTempView("test_execute")
    session.sql("select * from test_execute")
  }

  def test_conf(): Array[Any] = {
    session.conf.set("spark.executor.coresX",4)
    val cores_val = session.conf.get("spark.executor.coresX")
    session.conf.set("someProp",true)
    val someProp = session.conf.get("someProp")
    Array(cores_val, someProp)
  }

  def test_catalog_methods() : DataFrame = {
     val name = "TestDB"
     val tableName = "TestTable"
     val newDb = CatalogDatabase(name, name + " description", Utils.newUriForDatabase(), Map.empty)
     val catalog = session.catalog
     val sessionCatalog: SessionCatalog = session.sessionState.catalog
     val db = sessionCatalog.createDatabase(newDb, ignoreIfExists = false)
     catalog.setCurrentDatabase(name)   
     val table = Utils.newTable(tableName,Option(name))
     sessionCatalog.createTable(table, ignoreIfExists = true,validateLocation = false)
     val columns = catalog.listColumns(tableName)
     val d = df2Seq(columns.toDF())
     val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column: _*)
     df.createOrReplaceTempView("view1")
     catalog.dropTempView("view1")
     sessionCatalog.dropTable(TableIdentifier(tableName, Option(name)), ignoreIfNotExists = false, purge = false)
     sessionCatalog.dropDatabase(name, ignoreIfNotExists = false, cascade = true)
     columns.toDF().drop("description").drop("dataType").drop("isPartition").drop("isBucket")
  }



  //Main
  def main(args: Array[String]): Unit = {
    val packages = Package.getPackages()
    print(packages(0).getName())
    val l = packages(0).getName()
    print(l)
    // Spark testing main)
    /*var a = test_concat_ws()
    a.show()*/
    println("Done" )
  }

}
