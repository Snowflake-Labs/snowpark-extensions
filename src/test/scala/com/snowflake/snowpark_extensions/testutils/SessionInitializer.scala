package com.snowflake.snowpark_extensions.testutils

import com.snowflake.snowpark.Session
import com.snowflake.snowpark_extensions.testutils.Credentials._
import org.apache.spark.sql.SparkSession

object SessionInitializer {
  lazy val spark = SparkSession.builder()
    .master("local")
    .appName("Test Snowpark_Extensions")
    .getOrCreate()

  val configs = Map (
    "URL" -> s"https://$ACCOUNT.snowflakecomputing.com:443",
    "USER" -> USER,
    "PASSWORD" -> PASSWORD,
    "ROLE" -> ROLE,
    "WAREHOUSE" -> WAREHOUSE,
    "DB" -> DB,
    "SCHEMA" -> SPSCHEMA
  )

  lazy val snow = Session.builder.configs(configs).create
}
