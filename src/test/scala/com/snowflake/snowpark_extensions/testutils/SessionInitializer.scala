package com.snowflake.snowpark_extensions.testutils

import com.snowflake.snowpark.Session
import com.snowflake.snowpark_extensions.Extensions._

object SessionInitializer {
  // lazy val spark = SparkSession.builder()
  //   .master("local")
  //   .appName("Test Snowpark_Extensions")
  //   .getOrCreate()

 

  lazy val snow = Session.builder.from_snowsql().create
}
