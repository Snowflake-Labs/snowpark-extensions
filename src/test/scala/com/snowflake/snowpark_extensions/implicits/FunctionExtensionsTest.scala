
package com.snowflake.snowpark_extensions.implicits
import com.snowflake.snowpark.functions.{col, ltrim, rtrim}
import com.snowflake.snowpark_extensions.Extensions._
import com.snowflake.snowpark_extensions.Extensions.functions._
import com.snowflake.snowpark_extensions.helpers.BuiltinOverrides.{regexp_split, trim}
import com.snowflake.snowpark_extensions.testutils.Serializer.df2Seq
import com.snowflake.snowpark_extensions.testutils.SessionInitializer
import org.scalatest.{FlatSpec, Matchers}

class FunctionExtensionsTest extends FlatSpec with Matchers {
  behavior of "FunctionExtenions class"

  "when expression" should "compile without errors" in {
    when(col("a") > 1,"literal1").otherwise("literal2")
    val session = SessionInitializer.snow
    val df = session.createDataFrame(Seq(("aaaa"))).toDF(Seq("a"))
    df.withColumn("col1",when(col("a")>1,"a"))
    df.withColumn("col1",when(col("a")>1,"a").otherwise("b"))
  }

  "substring_index" should "match spark substring_index" in {
    // df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    // df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    // [Row(s='a.b')]
    // df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    // [Row(s='b.c.d')]
    val session = SessionInitializer.snow
    // Create an array of Row objects containing data.
    val data = Seq(("a.b.c.d"))
    // Define the schema for the columns in the DataFrame.
    import com.snowflake.snowpark_extensions.Extensions.functions._
    // Create the DataFrame.
    val df = session.createDataFrame(data).toDF(Seq("s"))
    Seq(Seq("a.b"))   shouldEqual df2Seq(df.select(substring_index(df("s"),".",2)))
  }

  "substring_index negative index" should "match spark substring_index" in {
    // df = spark.createDataFrame([('a.b.c.d',)], ['s'])
    // df.select(substring_index(df.s, '.', 2).alias('s')).collect()
    // [Row(s='a.b')]
    // df.select(substring_index(df.s, '.', -3).alias('s')).collect()
    // [Row(s='b.c.d')]
    val session = SessionInitializer.snow
    // Create an array of Row objects containing data.
    val data = Seq(("a.b.c.d"))
    // Define the schema for the columns in the DataFrame.
    import com.snowflake.snowpark_extensions.Extensions.functions._
    // Create the DataFrame.
    val df = session.createDataFrame(data).toDF(Seq("s"))
    Seq(Seq("b.c.d")) shouldEqual df2Seq(df.select(substring_index(df("s"),".",-3)))
  }

  "split" should "match spark split" in {


    val session = SessionInitializer.snow
    val df = session.createDataFrame(Seq("testAandtestBareTwoBBtests")).toDF {
      "s"
    }
    val res = df.select(regexp_split(df("s"), "test(A|BB)", 3)(session).alias("s")).collect()
    assert(res(0)(0) == "[\n  \"\",\n  \"andtestBareTwoBBtests\"\n]")

    val resExp = df.select(regexp_split(df("s"), "test(A|BB)", 1)(session).alias("s")).collect()
    assert(resExp(0)(0) == "[\n  \"testAandtestBareTwoBBtests\"\n]")

    val dfMail = session.createDataFrame(Seq("From: mauricio@mobilize.net")).toDF {
      "s"
    }
    val resMail = dfMail.select(regexp_split(dfMail("s"), "From: mauricio@mobilize.net")(session).alias("s")).collect()
    assert(resMail(0)(0) == "[\n  \"\",\n  \"\"\n]")

    val dfONe = session.createDataFrame(Seq("oneAtwoBthreeC")).toDF {
      "s"
    }
    val resOne = dfONe.select(regexp_split(dfONe("s"), "Z")(session).alias("s")).collect()
    assert(resOne(0)(0) == "[\n  \"oneAtwoBthreeC\"\n]")

    val resT = dfONe.select(regexp_split(dfONe("s"), "t")(session).alias("s")).collect()
    assert(resT(0)(0) == "[\n  \"oneA\",\n  \"woB\",\n  \"hreeC\"\n]")

    val resT1 = dfONe.select(regexp_split(dfONe("s"), "t", 1)(session).alias("s")).collect()
    assert(resT1(0)(0) == "[\n  \"oneAtwoBthreeC\"\n]")

    val resT2 = dfONe.select(regexp_split(dfONe("s"), "t", 2)(session).alias("s")).collect()
    assert(resT2(0)(0) == "[\n  \"oneA\",\n  \"woBthreeC\"\n]")


    val resABC = dfONe.select(regexp_split(dfONe("s"), "[ABC]")(session).alias("s")).collect()
    assert(resABC(0)(0) == "[\n  \"one\",\n  \"two\",\n  \"three\",\n  \"\"\n]")

    val resABC1 = dfONe.select(regexp_split(dfONe("s"), "[ABC]", 1)(session).alias("s")).collect()
    assert(resABC1(0)(0) == "[\n  \"oneAtwoBthreeC\"\n]")

    val dfHello = session.createDataFrame(Seq("HelloabNewacWorld")).toDF {
      "s"
    }
    val resHello = dfHello.select(regexp_split(dfHello("s"), "a([b, c]).*?")(session).alias("s")).collect()
    assert(resHello(0)(0) == "[\n  \"Hello\",\n  \"New\",\n  \"World\"\n]")


    val dfDate = session.createDataFrame(Seq("HelloabNewacWorld")).toDF {
      "s"
    }
    val resDate = dfDate.select(regexp_split(dfDate("s"), "\\w+.")(session).alias("s")).collect()
    assert(resDate(0)(0) == "[\n  \"\",\n  \"\"\n]")

    val dfLine = session.createDataFrame(Seq("""line 1 line 2 line 3""")).toDF {
      "s"
    }
    val resLine = dfLine.select(regexp_split(dfLine("s"), "\\n", 3)(session).alias("s")).collect()
    assert(resLine(0)(0) == "[\n  \"line 1 line 2 line 3\"\n]")

    val dfLine1 = session.createDataFrame(Seq("line 1(\\n)")).toDF {
      "s"
    }
    val resLine1 = dfLine1.select(regexp_split(dfLine1("s"), "line 1(\\n)", 3)(session).alias("s")).collect()
    assert(resLine1(0)(0) == "[\n  \"line 1(\\\\n)\"\n]")

    val dfPin = session.createDataFrame(Seq("The price of PINEAPPLE ice cream is 20")).toDF {
      "s"
    }
    val resPin = dfLine.select(regexp_split(dfPin("s"), "(\\b[A-Z]+\\b).+(\\b\\d+)", 4)(session).alias("s")).collect()
    assert(resPin(0)(0) == "[\n  \"line 1 line 2 line 3\"\n]")


    val dfBut = session.createDataFrame(Seq("<button type=\"submit\" class=\"btn\">Send</button>")).toDF {
      "s"
    }
    val resBut = dfBut.select(regexp_split(dfBut("s"), "\".+?\"", 4)(session).alias("s")).collect()
    assert(resBut(0)(0) == "[\n  \"<button type=\",\n  \" class=\",\n  \">Send</button>\"\n]")
  }

  "trim" should "match spark trim" in {

    val session = SessionInitializer.snow
    val df = session.createDataFrame(Seq("testAandtestBareTwoBBtests ")).toDF {"s"}
    val res = df.select(trim(df("s")).alias("s")).collect()
    assert(res(0)(0) == "testAandtestBareTwoBBtests")

    val dfAll = session.createDataFrame(Seq(" testAandtestBareTwoBBtests ")).toDF {"s"}
    val resAll = dfAll.select(trim(dfAll("s")).alias("s")).collect()
    assert(resAll(0)(0) == "testAandtestBareTwoBBtests")

    val dfMiddle = session.createDataFrame(Seq(" testAandtest   BareTwoBBtests ")).toDF {"s"}
    val resMiddle = dfMiddle.select(trim(dfMiddle("s")).alias("s")).collect()
    assert(resAll(0)(0) == "testAandtestBareTwoBBtests")

    val dfSym = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {"s"}
    val resSym = dfSym.select(trim(dfSym("s")).alias("s")).collect()
    assert(resSym(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")

    val dfSymRight = session.createDataFrame(Seq("test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {"s"}
    val resSymRight = dfSymRight.select(trim(dfSymRight("s")).alias("s")).collect()
    assert(resSymRight(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")

    val dfSymLeft = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")).toDF {"s"}
    val resSymLeft = dfSymLeft.select(trim(dfSymLeft("s")).alias("s")).collect()
    assert(resSymLeft(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")
  }

  "rtrim" should "match spark rtrim" in {

    val session = SessionInitializer.snow
    val df = session.createDataFrame(Seq("testAandtestBareTwoBBtests ")).toDF {
      "s"
    }
    val res = df.select(rtrim(df("s")).alias("s")).collect()
    assert(res(0)(0) == "testAandtestBareTwoBBtests")

    val dfAll = session.createDataFrame(Seq(" testAandtestBareTwoBBtests ")).toDF {
      "s"
    }
    val resAll = dfAll.select(rtrim(dfAll("s")).alias("s")).collect()
    assert(resAll(0)(0) == " testAandtestBareTwoBBtests")

    val dfMiddle = session.createDataFrame(Seq(" testAandtest   BareTwoBBtests ")).toDF {
      "s"
    }
    val resMiddle = dfMiddle.select(rtrim(dfMiddle("s")).alias("s")).collect()
    assert(resAll(0)(0) == " testAandtestBareTwoBBtests")

    val dfSym = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {
      "s"
    }
    val resSym = dfSym.select(rtrim(dfSym("s")).alias("s")).collect()
    assert(resSym(0)(0) == " test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")

    val dfSymRight = session.createDataFrame(Seq("test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {
      "s"
    }
    val resSymRight = dfSymRight.select(rtrim(dfSymRight("s")).alias("s")).collect()
    assert(resSymRight(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")

    val dfSymLeft = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")).toDF {
      "s"
    }
    val resSymLeft = dfSymLeft.select(rtrim(dfSymLeft("s")).alias("s")).collect()
    assert(resSymLeft(0)(0) == " test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")
  }

  "ltrim" should "match spark ltrim" in {

    val session = SessionInitializer.snow
    val df = session.createDataFrame(Seq("testAandtestBareTwoBBtests ")).toDF {
      "s"
    }
    val res = df.select(ltrim(df("s")).alias("s")).collect()
    assert(res(0)(0) == "testAandtestBareTwoBBtests ")

    val dfAll = session.createDataFrame(Seq(" testAandtestBareTwoBBtests ")).toDF {
      "s"
    }
    val resAll = dfAll.select(ltrim(dfAll("s")).alias("s")).collect()
    assert(resAll(0)(0) == "testAandtestBareTwoBBtests ")

    val dfMiddle = session.createDataFrame(Seq(" testAandtest   BareTwoBBtests ")).toDF {
      "s"
    }
    val resMiddle = dfMiddle.select(ltrim(dfMiddle("s")).alias("s")).collect()
    assert(resAll(0)(0) == "testAandtestBareTwoBBtests ")

    val dfSym = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {
      "s"
    }
    val resSym = dfSym.select(ltrim(dfSym("s")).alias("s")).collect()
    assert(resSym(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")

    val dfSymRight = session.createDataFrame(Seq("test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")).toDF {
      "s"
    }
    val resSymRight = dfSymRight.select(ltrim(dfSymRight("s")).alias("s")).collect()
    assert(resSymRight(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests ")

    val dfSymLeft = session.createDataFrame(Seq(" test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")).toDF {
      "s"
    }
    val resSymLeft = dfSymLeft.select(ltrim(dfSymLeft("s")).alias("s")).collect()
    assert(resSymLeft(0)(0) == "test$#\"test   Ba$#\"$#\"$#\"$#\"reTwoBBtests")
  }

}