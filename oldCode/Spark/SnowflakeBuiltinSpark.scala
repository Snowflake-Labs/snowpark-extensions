package com.snowflake.snowpark_extensions.helpers.Spark

import com.snowflake.snowpark_extensions.testutils.{DataFrameCreator, SessionInitializer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.snowflake.snowpark
import org.apache.spark.sql.expressions.Window

object SnowflakeBuiltinSpark {

    // Init
    val session = SessionInitializer.spark
    val df = session.createDataFrame(DataFrameCreator.data_for_general).toDF(DataFrameCreator.data_for_general_column:_*)
    val df_array = session.createDataFrame(DataFrameCreator.data_for_array).toDF(DataFrameCreator.data_for_array_column:_*)
    val df_for_json = session.createDataFrame(DataFrameCreator.data_for_json).toDF(DataFrameCreator.data_for_json_column:_*)
    val df_for_json_complex = session.createDataFrame(DataFrameCreator.data_for_json_complex).toDF(DataFrameCreator.data_for_json_complex_column: _*)
    val df_for_cast = session.createDataFrame(DataFrameCreator.data_for_date_cast).toDF(DataFrameCreator.data_for_date_cast_column: _*)
    val df_for_rounding = session.createDataFrame(DataFrameCreator.data_for_rounding).toDF(DataFrameCreator.data_for_rounding_column: _*)
    val df_for_regex = session.createDataFrame(DataFrameCreator.data_for_regex).toDF(DataFrameCreator.data_for_regex_column: _*)
    val df_for_signum = session.createDataFrame(DataFrameCreator.data_for_signum).toDF(DataFrameCreator.data_for_signum_column: _*)
    val df_for_double  = session.createDataFrame(DataFrameCreator.data_for_double).toDF(DataFrameCreator.data_for_double_column:_*)
    val df_for_double2  = session.createDataFrame(DataFrameCreator.data_for_double2).toDF(DataFrameCreator.data_for_double2_column:_*)
    val df_for_base64 = session.createDataFrame(DataFrameCreator.data_for_base64).toDF(DataFrameCreator.data_for_base64_column:_*)
    val dfString = session.createDataFrame(DataFrameCreator.data_for_float).toDF(DataFrameCreator.data_for_float_column:_*)
    val df_conv = session.createDataFrame(DataFrameCreator.data_for_conv).toDF(DataFrameCreator.data_for_conv_column:_*)
    val df_window = session.createDataFrame(DataFrameCreator.data_for_window).toDF(DataFrameCreator.data_for_window_column:_*)

  //size
    def test_size(): DataFrame = {
        df_array.select(size(col("col1")).as("result"))
    }

    //expr    
    def test_expr(): DataFrame = {
        df.select(expr("col2").as("result"))
    }

    //column
    def test_column(): DataFrame = {
        df.select(column("col1").as("result"))
    }

    //array
    def test_array(): DataFrame = {
        df.select(array(col("col1"),col("col2"),col("col3")).as("result"))
    }

    //concat
    def test_concat(): DataFrame = {
        df.select(concat(col("col1"),col("col2"),col("col3")).as("result"))
    }

    ///////////////////////////////////// Mauricio ////////////////////////////////////////////////
    def test_nanvl() = {
        df_for_double.select(nanvl(col("col1"), lit(0)).alias("r1"))
    }

    def test_log10() = {
        df_for_double2.select(log10(col("col1")).cast(IntegerType).alias("r1"))
    }

    def test_log1p() = {
        df_for_double2.select(log1p(col("col1")).cast(IntegerType).alias("r1"))
    }

    def test_base64() = {
        df_for_base64.withColumn("xxx", base64(col("col1")))
    }

     def test_unbase64() = {
        df_for_base64.withColumn("xxx", base64(col("col1"))).withColumn("yyy",unbase64(col("xxx")).cast(StringType))
    }

    def test_locate() = {
        val df = session.createDataFrame(Seq(("bar","foobarbar",0),("bar","foobarbar",1))).toDF("substr","str","pos")
        df.select(locate("bar",col("str"),0).as("first"), locate("bar",col("str"),6).as("first"))
    }

    def test_format_string() = {
        val df = session.createDataFrame(Seq((5, "hello"))).toDF("a", "b")
        df.select(format_string("%d %s", col("a"), col("b")).alias("v"))
    }

    ///////////////////////////////////// Boga ////////////////////////////////////////////////

    def test_shiftleft() = {
        df_for_json.select(
            col("col2")
            , shiftleft(col("col2").cast(LongType), 5)
            , shiftleft(col("col2").cast(LongType), 7)
            , shiftleft(col("col2").cast(LongType), 12)
        )
    }

    def test_shiftright() = {
        df_for_json.select(
            col("col2").alias("Num")
            , shiftright(col("col2").cast(LongType), 5)
            , shiftright(col("col2").cast(LongType), 7)
            , shiftright(col("col2").cast(LongType), 12)
        )
    }

    def test_hex(): DataFrame = {
        df_for_json.toJSON.select(hex(lower(col("value"))).as("hex_value"))
    }

    def test_unhex(): DataFrame = {
        test_hex().withColumn("string_val", decode(unhex(col("hex_value")), "utf-8"))
    }

    def test_json_tuple(): DataFrame = {
        df_for_json_complex
          .select(json_tuple(col("col2"), "int", "float", "boolean", "empty", "str", "long", "dbl", "time", "date", "datetime"))
          .select(
              col("c0").cast(IntegerType)
              // , col("c1").cast(FloatType) Omitted due to differences in the precision. Difference is documented in the json_tuple function in SnowflakeBuiltin.scala
              , col("c2").cast(BooleanType)
              , col("c3").cast(IntegerType)
              , col("c4").cast(StringType)
              , col("c5").cast(LongType)
              , col("c6").cast(DoubleType)
              , col("c8").cast(DateType)
              // , col("c9").cast(TimestampType) Omitted due to difference when casting the date. Difference is documented in the json_tuple function in SnowflakeBuiltin.scala
          )
    }

    def test_cbrt(): DataFrame = {
        df_for_json.select(round(cbrt("col2"), 10))
    }

    def test_from_json(): DataFrame = {
        val schema = new StructType()
          .add("id", IntegerType)
          .add("age", IntegerType)
          .add("relative", MapType(StringType, IntegerType))

        val json_df = df_for_json.select(
            from_json(col("col4"), schema).as("json")
        )

        json_df.select(
            "json.id"
            , "json.age"
            , "json.relative.id"
            , "json.relative.age"
        )
    }

    def test_from_jsonAllDatatypes(): DataFrame = {
        val schema = new StructType()
          .add("int", IntegerType)
          //.add("float", FloatType) Omitted due to differences in the precision. Difference is documented in the json_tuple function in SnowflakeBuiltin.scala
          .add("boolean", BooleanType)
          .add("empty", IntegerType)
          .add("str", StringType)
          .add("long", LongType)
          .add("dbl", DoubleType)
          .add("date", DateType)
          //.add("datetime", TimestampType) Omitted due to difference when casting the date. Difference is documented in the json_tuple function in SnowflakeBuiltin.scala
          .add("relative", MapType(StringType, IntegerType))
        val json_df = df_for_json_complex.select(
            from_json(col("col2"), schema).as("json")
        )
        json_df.select(
            "json.int"
            , "json.boolean"
            , "json.empty"
            , "json.str"
            , "json.long"
            , "json.dbl"
            , "json.date"
            , "json.relative.age"
        )
    }

    def test_date_sub() = {
        var df_test = df_for_cast.selectExpr(
            "to_date(col5, 'yyyy-MM-dd') as date_only"
            , "to_timestamp(col6, 'MM/dd/yyyy HH:mm:ss') as date_time"
            , "col5 as date_only_str"
            , "col6 as date_time_str"
        )
        df_test
          .withColumn("date_date_sub", date_sub(col("date_only"), 5))
          .withColumn("datetime_date_sub", date_sub(col("date_time"), 7))
          .withColumn("date_date_sub_str", date_sub(col("date_only_str"), 5))
          // .withColumn("datetime_date_str", date_sub(col("date_time_str"), 7)) Omitted due to functional differences. Difference is documented in the date_sub function in SnowflakeBuiltin.scala
          .withColumn("date_date_sub_neg", date_sub(col("date_only"), -5))
          .withColumn("datetime_date_sub_neg", date_sub(col("date_time"), -7))
    }

    def test_bround(): DataFrame = {
        df_for_rounding
          .select(
              bround(col("col1"))
              , bround(col("col1"), 1)
              , bround(col("col1"), 2)
              , bround(col("col1"), -1)
              , bround(col("col2"))
              , bround(col("col2"), 1)
              , bround(col("col2"), 2)
              , bround(col("col2"), -1)
          )
    }

    def test_regexp_extract(): DataFrame = {
        df_for_regex.select(
            regexp_extract(col("col1"), "(This \\w+|test|\\w+\\s+nice.$)", 1)
            , regexp_extract(col("col1"), "(This.*?\\.)\\s+(Some.*)", 1)
            , regexp_extract(col("col1"), "(This.*?\\.)\\s+(Some.*)", 2)
            , regexp_extract(col("col2"), "(This|^That|test\\.$)", 1)
            , regexp_extract(col("col2"), "(is.*is)", 1)
        )
    }

    def test_signum(): DataFrame = {
        df_for_signum.select(
            signum("col1")
            , signum("col2")
        )
    }

    ///////////////////////////////////// Tannia ////////////////////////////////////////////////

    //date_format(dateExpr: Column, format: String): Column
    def test_date_format(): DataFrame = {
        /*df_date.select(date_format(col("col1"),"yyyyMMdd"), date_format(col("col1"),"yyyy-MM-dd"), date_format(col("col1"),"yyyy.MM.dd"),
                       date_format(col("col2"),"yyyyMMdd HH:mm:ss"), date_format(col("col2"),"yyyy-MM-dd HH:mm:ss"), date_format(col("col2"),"yyyy.MM.dd HH.mm.ss"))*/
        df
    }



    ///////////////////////////////////// Fonse ////////////////////////////////////////////////

    //array String
    def test_arrayString(): DataFrame = {
        df.select(array("col1","col2","col3").as("result"))
    }	
	
    //last column
    def test_last(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1"))
      df_window.orderBy(col("col1"),col("col2")).withColumn("mycol", last(col("col2")) over (partitionWindow))
    }

    //last string
    def test_last_string(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", last("col2") over (partitionWindow))
    }

    //collect_list
    def test_collect_list() : DataFrame = {
      df.groupBy("col1").agg(collect_list(col("col2")).as("mycol"))
    }

    //collect_list String
    def test_collect_list_string() : DataFrame = {
      df.groupBy("col1").agg(collect_list("col2").as("mycol"))
    }

    //reverse
    def test_reverse() : DataFrame = {
      dfString.select(reverse(dfString.col("col3")).alias("mycol"))
    }

    //isnull
    def test_isnull() : DataFrame = {
      df.select(isnull(col("col1")).alias("mycol1"), isnull(col("col3")).alias("mycol3"))
    }

    //last ignoreNullsString
    def test_last_ignoreNullsString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", last("col2", true) over (partitionWindow))
    }

    //last ignoreNulls
    def test_last_ignoreNulls(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", last(col("col2"), true) over (partitionWindow))
    }

    //first
    def test_first(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"),col("col2"))
      df_window.withColumn("mycol", first(col("col2")) over (partitionWindow))
    }

    //first string
    def test_first_string(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"),col("col2"))
      df_window.withColumn("mycol", first("col2") over (partitionWindow))
    }

    //first ignoreNullsString
    def test_first_ignoreNullsString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"),col("col2"))
      df_window.withColumn("mycol", first("col2", true) over (partitionWindow))
    }

    //first ignoreNulls
    def test_first_ignoreNulls(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"), col("col2"))
      df_window.withColumn("mycol", first(col("col2"), true) over (partitionWindow))
    }

    //lag
    def test_lag(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag(col("col2"),1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagString
    def test_lagString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag("col2",1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagDefault
    def test_lagDefault(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag(col("col2"),1,0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lagDefaultString
    def test_lagDefaultString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lag("col2",1, 0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //lead
    def test_lead(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead(col("col2"),1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadString
    def test_leadString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead("col2",1) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadDefault
    def test_leadDefault(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead(col("col2"),1,0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //leadDefaultString
    def test_leadDefaultString(): DataFrame = {
      val partitionWindow = Window.partitionBy(col("col1")).orderBy(col("col1"))
      df_window.withColumn("mycol", lead("col2",1, 0) over (partitionWindow)).orderBy(col("col1"),col("col2"))
    }

    //conv
    def test_conv() : DataFrame = {
      df_conv.select(conv(col("col1"), 2, 16).alias("mycol"))
    }

  ///////////////////////////////////// Jeremy ////////////////////////////////////////////////


    //Main
    def main(args: Array[String]): Unit = {
        // Spark testing main
      test_conv.show
    }

}
