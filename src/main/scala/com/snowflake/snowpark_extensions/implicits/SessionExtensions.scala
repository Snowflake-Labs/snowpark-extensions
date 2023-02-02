/**
  Copyright (C) Mobilize.Net info@mobilize.net - All Rights Reserved

  This file is part of the Mobilize Frameworks, which is
  proprietary and confidential.

  NOTICE:  All information contained herein is, and remains
  the property of Mobilize.Net Corporation.
  The intellectual and technical concepts contained herein are
  proprietary to Mobilize.Net Corporation and may be covered
  by U.S. Patents, and are protected by trade secret or copyright law.
  Dissemination of this information or reproduction of this material
  is strictly forbidden unless prior written permission is obtained
  from Mobilize.Net Corporation.
*/
package com.snowflake.snowpark_extensions.implicits

import com.snowflake.snowpark.{DataFrame, Session, Row}
import scala.collection.mutable.WeakHashMap
import com.snowflake.snowpark.types.StructField
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.types.ColumnIdentifier
import com.snowflake.snowpark.types.DataType
import com.snowflake.snowpark.types.IntegerType
import com.snowflake.snowpark.types.StringType
import com.snowflake.snowpark.types.BooleanType
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.io.File
import org.ini4j.Wini

/**
 * The options tables holds configurations values, in a similar way as the SparkSession.conf class.
 */
class OptionsTable 
{
  /**
   * Options values are stored here 
   */
  val options = new WeakHashMap[String,String]()
  /**
   *  Gets the currently stored value for the given option key
   *  @param opt String key for the property
   *  @return String Data for the given property
   */
  def get(opt:String) : String = {
    return options.getOrElse(opt,null)
  }
  /**
   * Set the value for key using the given option key and value
   * @param opt String key for the property
   * @param value Any the value to be stored. Values will be stored as strings 
   */
  def set(opt:String,value:Any):Any = {
    val currentVal = options.getOrElse(opt,null)
    if (currentVal == null)
    {
      options += opt -> value.toString()
    }
    return value
  }
}

/**
 * The Catalog class provides APIS to access the snowflake catalog tables, to get information about the 
 * object in the database
 */
class Catalog(session:Session) {
  /**
   * Creates a database with the given name.
   * NOTE: the user must have permissions to create a database
   * @param String name Name of the database to create
   */
  def createDatabase(name:String) : Unit = {
    session.sql(s"""CREATE DATABASE IF NOT EXISTS "$name" """).count()
  }
  /**
   * Sets the current database. 
   * NOTE: the user must have permissions to use that database
   * @param String name Name of the database
   */
  def setCurrentDatabase(name:String) : Unit = {
    session.sql(s"USE DATABASE ${name}").count()
  }

  /**
   * Schema for the information returned from listColumns. 
   * This schema matches the Spark Schema for listColumns for compatibility
   */
  lazy val columnsSchema = 
    StructType(
    Array(
      StructField(ColumnIdentifier("name"),StringType,true),
      StructField(ColumnIdentifier("description"),StringType,true),
      StructField(ColumnIdentifier("dataType"),StringType,true),
      StructField(ColumnIdentifier("nullable"),BooleanType,true),
      StructField(ColumnIdentifier("isPartition"),BooleanType,true),
      StructField(ColumnIdentifier("isBucket"),BooleanType,true)
    ))

  /**
   * Returns a DataFrame with information about the columns on a table
   * @param tableName String The name of the table
   */
  def listColumns(tableName:String) : DataFrame = {     
   val rows = session.table(tableName).schema.fields.map(f => Row.fromSeq(Seq(f.name.replaceAll("^\"|\"$", ""), null,f.dataType.typeName,f.nullable,null,null)))
   session.createDataFrame(rows,columnsSchema)
  }

  /**
   * Drops a temporary view
   * @param viewName String teh name of the view
   */
  def dropTempView(viewName:String) : Unit = {
    session.sql(s"DROP VIEW $viewName").count()
  }
}

/** Session Extensions object containing implicit functions to the Snowpark Session object. */
object SessionExtensions {
  private val sessionOptions = new WeakHashMap[Session,OptionsTable]
  /**
   * Session extension class.
   * @param s Session to extend functionality.
   */
  class ExtendedSession (s: Session){
  

  /**
   * Executes some code block and prints to stdout the time taken to execute the block. 
   * @param f T Function to be called
   */
  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${NANOSECONDS.toMillis(end - start)} ms")
    // scalastyle:on println
    ret
  }

    /**
     * Executes the input SQL query text in Snowflake. Used for Hive Context.
     * @param query Query text to execute.
     * @return DataFrame resulting from the query.
     */
    def execute(query : String): DataFrame = {
      s.sql(query)
    }

    /**
     * Provides access to the catalog
     */
    lazy val catalog = new Catalog(s)

    /**
     * Provide a table to store options.
     * NOTE: there are many options that have no effect in snowflake
     */
    def conf() : OptionsTable = {
      var confSessions:OptionsTable = sessionOptions.getOrElse(s,null)
      if (confSessions == null) {
        confSessions = new OptionsTable
        sessionOptions += s -> confSessions
      }
      return confSessions
    }

    /**
     * Executes the input SQL update query text in Snowflake. Used for Hive Context.
     * @param query Update query text to execute.
     */
    def executeUpdate(query : String): Unit = {
      s.sql(query).count()
    }
  }

   class ExtendedSessionBuilder (private var sb: Session.SessionBuilder) {

   private val homeDir = System.getProperty("user.home")

   private val sep = System.getProperty("file.separator")

   private def loadFromEnvIfPresent(configKey:String,env1:String, env2:String):Session.SessionBuilder = {
        var value = sys.env.get(env1)
        if (value.isDefined) {
          return sb.config(configKey,value.get)
        }
        else {
           value = sys.env.get(env2)
          if (value.isDefined) {
            return sb.config(configKey,value.get)
          }
        }
        return sb
   }

   private def loadFromEnvIfPresentURL(configKey:String,env1:String, env2:String):Session.SessionBuilder={
        var value = sys.env.get(env1)
        if (value.isDefined) {
            return sb.config(configKey,s"https://${value.get}.snowflakecomputing.com:443")
        }
        else {
          value = sys.env.get(env2)
          if (value.isDefined) {
            return sb.config(configKey,s"https://${value.get}.snowflakecomputing.com:443")
          }
        }
        return sb
    }
    
    private def loadFromIni(configKey:String,ini:Wini,sectionName:String,key:String):Session.SessionBuilder={
      var value = ini.get(sectionName,key)
      if (value!=null){
        sb = sb.config(configKey,value)
      }
      return sb
    }

    private def loadFromIniURL(ini:Wini,sectionName:String,key:String):Session.SessionBuilder={
      var value = ini.get(sectionName,key)
      if (value!=null){
        val newValue = s"https://${value}.snowflakecomputing.com:443"
        sb = sb.config("url",newValue)
        sb = sb.config("account",value)
      }
      return sb
    }

    /**
     * utility method that sets up the connection properties from SNOW_xxx or SNOWSQL_xxx environment variables
     * if they are set
     */   
   def from_env() = {
        sb = loadFromEnvIfPresent("user"      ,"SNOW_USER"     , "SNOWSQL_USER")
        sb = loadFromEnvIfPresent("password"  ,"SNOW_PASSWORD" , "SNOWSQL_PWD")
        sb = loadFromEnvIfPresent("account"   ,"SNOW_ACCOUNT"  , "SNOWSQL_ACCOUNT")
        sb = loadFromEnvIfPresentURL("url"    ,"SNOW_ACCOUNT"  , "SNOWSQL_ACCOUNT")
        sb = loadFromEnvIfPresent("role"      ,"SNOW_ROLE"     , "SNOWSQL_ROLE")
        sb = loadFromEnvIfPresent("warehouse" ,"SNOW_WAREHOUSE", "SNOWSQL_ROLE")
        sb = loadFromEnvIfPresent("database"  ,"SNOW_DATABASE" , "SNOWSQL_DATABASE")
        sb
    }  

    /**
     * utility method that sets up the connection properties from the snowsql config file
     */   
  def from_snowsql(section_name:String=null,configpath:String=s"${homeDir}${sep}.snowsql${sep}config") = {
        var config_section_name = "connections" 
        val file = new File(configpath)
        if (file.exists())
        {
          val ini = new Wini(file)
          
          if (section_name != null) {
            config_section_name = f"connections.{section}"
          }
          sb = loadFromIni("user",ini,config_section_name,"username")
          sb = loadFromIni("password",ini,config_section_name,"password")
          sb = loadFromIni("role",ini,config_section_name,"rolename")
          sb = loadFromIni("warehouse",ini,config_section_name,"warehousename")
          sb = loadFromIni("database",ini,config_section_name,"dbname")
          sb = loadFromIni("schema",ini,config_section_name,"schemaname")
          sb = loadFromIniURL(ini,config_section_name,"accountname")
        }
        else {
          println(s"Config section ${config_section_name} not found in snowsql file: ${configpath}")
        }
        sb
  }
}
}

