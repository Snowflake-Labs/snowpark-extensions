/**
  * Snowpark is similar in many things to Spark but not in all.
  * There are some functionality that might be different.
  * 
  * The purpose of this library is to 
  */
package com.snowflake.snowpark_extensions


import com.snowflake.snowpark.Column
import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.CaseExpr
import com.snowflake.snowpark_extensions.implicits.ColumnExtensions
import com.snowflake.snowpark_extensions.implicits.DataFrameExtensions
import com.snowflake.snowpark_extensions.implicits.SessionExtensions
import com.snowflake.snowpark_extensions.implicits.CaseExprExtensions
import com.snowflake.snowpark.Session

object Extensions {
  import scala.language.implicitConversions
  implicit def extendedColumn(c: Column) =
    new ColumnExtensions.ExtendedColumn(c)
  implicit def extendedDataFrame(df: DataFrame) =
    new DataFrameExtensions.ExtendedDataFrame(df)
  implicit def extendedSession(s: Session) =
    new SessionExtensions.ExtendedSession(s)
  implicit def extendedCaseExpr(cex: CaseExpr) =
    new CaseExprExtensions.ExtendedCaseExpr(cex)



/**  
 * Collection of helper functions to provide more equivalency between snowpark and spark
 */
object functions {
  import com.snowflake.snowpark.{Column, DataFrame, functions}
  import com.snowflake.snowpark.functions._
  import com.snowflake.snowpark.types._
  /**
   * Function to convert a string into an SQL expression.
   * @param s SQL Expression as text.
   * @return Converted SQL Expression.
   */
  def expr(s: String) = sqlExpr(s)

  /**
   * Function to convert column name into column and order in a descending manner.
   * @param c Column name.
   * @return Column object ordered in a descending manner.
   */
  def desc(c:String) = col(c).desc

  /**
   * Function to convert column name into column and order in an ascending manner.
   * @param colname Column name.
   * @return Column object ordered in an ascending manner.
   */
  def asc(colname: String) = col(colname).asc

  /**
   * Wrapper for Snowflake built-in size function. Gets the size of array column.
   * @param c Column to get the size.
   * @return Size of array column.
   */
  def size(c: Column) = array_size(c)

  /**
   * Wrapper for Snowflake built-in array function. Create array from columns.
   * @param c Columns to build the array.
   * @return The array.
   */
  def array(c: Column*) = array_construct(c:_*)

  /**
   * Wrapper for Snowflake built-in date_format function. Converts a date into a string using the specified format.
   * @param c Column to convert to string.
   * @param s Date format.
   * @return Column object.
   */
  def date_format(c: Column, s: String) =
    builtin("to_varchar")(c.cast(TimestampType), s.replace("mm","mi"))

  /**
   * Wrapper for Snowflake built-in first function. Gets the first value of a column according to its grouping.
   * @param c Column to obtain first value.
   * @return Column object.
   */
  def first(c: Column) =
    builtin("FIRST_VALUE")(c)

  /**
   * Wrapper for Snowflake built-in last function. Gets the last value of a column according to its grouping.
   * Functional difference with windows, In Snowpark is needed the order by. SQL doesn't guarantee the order.
   * @param c Column to obtain last value.
   * @return Column object.
   */
  def last(c: Column) =
    builtin("LAST_VALUE")(c)

  ///////////////////////////////////// Mauricio ////////////////////////////////////////////////

/**
  * Formats the arguments in printf-style and returns the result as a string column.
  *
  * @note this function requires the format_string UDF to be previosly created
  * @param format the printf-style format
  * @param arguments arguments for the formatting string
  * @return formatted string
  */
  def format_string(format: String, arguments: Column*): Column = {
    callBuiltin("format_string",lit(format),array_construct(arguments:_*))
  }

 /**
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   * @note The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str. This function is just leverages the SF POSITION builtin
   *
   * @param substr string to search
   * @param str value where string will be searched
   * @param pos index for starting the search
   * @return Returns the position of the first occurrence
   */
  def locate(substr:String,str:Column,pos:Int = 0) = if (pos ==0) lit(0) else callBuiltin("POSITION",lit(substr),str,lit(pos))

  /**
    * 
    * Locate the position of the first occurrence of substr in a string column, after position pos.
    * @note The position is not zero based, but 1 based index. returns 0 if substr
    * could not be found in str. This function is just leverages the SF POSITION builtin
    * @param substr string to search
    * @param str value where string will be searched
    * @param pos index for starting the search
    * @return returns the position of the first occurrence. 
    */
    def locate(substr:Column,str:Column,pos:Int) = if (pos == 0) lit(0) else callBuiltin("POSITION",substr,str,pos)
  /**
   * Computes the logarithm of the given column in base 10.
   *
   * @param expr Column to apply this mathematical operation
   *
   * @return log2 of the given column
   */
  def log10(expr: Column): Column = builtin("LOG")(10,expr)

  /**
   * Computes the logarithm of the given column in base 10.
   *
   * @param columnName Column to apply this mathematical operation
   *
   * @return log2 of the given column
   */
  def log10(columnName: String): Column = builtin("LOG")(10,col(columnName))

  /**
    * Computes the natural logarithm of the given value plus one.
    *
    * @param columnName the value to use
    * @return the natural logarithm of the given value plus one.
    */
  def log1p(columnName:String) : Column = callBuiltin("ln", lit(1) + col(columnName))


  /**
    * Computes the natural logarithm of the given value plus one.
    *
    * @param col the value to use
    * @return the natural logarithm of the given value plus one.
    */

  def log1p(col:Column) : Column = callBuiltin("ln", lit(1) + col)

  /**
  * Returns expr1 if it is not NaN, or expr2 if expr1 is NaN.
  * @param expr1 expression when value is NaN
  * @param expr2 expression when value is not NaN
  */
  def nanvl(expr1:Column, expr2:Column): Column = callBuiltin("nanvl", expr1.cast(FloatType),expr2.cast(FloatType)).cast(FloatType)

  /**
    * Computes the BASE64 encoding of a column
    *
    * @param col
    * @return the encoded column
    */
  def base64(col:Column) : Column = callBuiltin("BASE64_ENCODE",col)


  /**
    * Decodes a BASE64 encoded string
    * @param col
    * @return the decoded column
    */
  def unbase64(col:Column) : Column = callBuiltin("BASE64_DECODE_STRING",col)

  // /**
  //  * Window function: returns the ntile group id (from 1 to `n` inclusive) in an ordered window
  //  * partition. For example, if `n` is 4, the first quarter of the rows will get value 1, the second
  //  * quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
  //  *
  //  * This is equivalent to the NTILE function in SQL.
  // * @param n number of groups
  // * @return retyr
  // */
   def ntile(n: Int): Column = callBuiltin("ntile",lit(n))


  ///////////////////////////////////// Boga ////////////////////////////////////////////////

  /**
   * Wrapper for bitshiftleft. Shifts numBits bits of a number to the left. There is a slight difference between Spark and
   * Snowflake's implementation. When shifting an integer value, if Snowflake detects that the shift will exceed the maximum value for the
   * integers, it will convert it to Long, whereas Spark will make it negative and start counting from the bottom.
   * Example: shiftleft(2147483647, 5) returns -32 on Spark, but on Snowflake it returns 68719476704. This is fixed by casting the column
   * in Spark to LongType.
   * In general, this difference is accepted.
   * @param c Column to modify.
   * @param numBits Number of bits to shift.
   * @return Column object.
   */
  def shiftleft(c: Column, numBits: Int): Column =
    bitshiftleft(c, lit(numBits))

  /**
   * Wrapper for bitshiftright. Shifts numBits bits of a number to the right. There might be differences on this function
   * similar to the ones explained on shiftleft.
   * @param c Column to modify.
   * @param numBits Number of bits to shift.
   * @return Column object.
   */
  def shiftright(c: Column, numBits: Int): Column =
    bitshiftright(c, lit(numBits))

  /**
   * Wrapper for Snowflake built-in hex_encode function. Returns the hexadecimal representation of a string.
   * @param c Column to encode.
   * @return Encoded string.
   */
  def hex(c: Column): Column =
    builtin("HEX_ENCODE")(c)

  /**
   * Wrapper for Snowflake built-in hex_decode_string function. Returns the string representation of a hexadecimal value.
   * @param c Column to encode.
   * @return Encoded string.
   */
  def unhex(c: Column): Column =
    builtin("HEX_DECODE_STRING")(c)

  /**
   * Wrapper for Spark randn. It will return a call to the Snowflake RANDOM function. The return values differ from Snowflake to Spark, however this difference is accepted.
   * Spark returns a Float number between -9 and 9 with 15-17 floating points, whereas Snowflake returns integers of 17-19 digits.
   * @return Random number.
   */
  def randn(): Column =
    builtin("RANDOM")()

  /**
   * Wrapper for Spark randn(seed). It will return a call to the Snowflake RANDOM function. The return values differ from Snowflake to Spark, however this difference is accepted.
   * Spark returns a Float number between -9 and 9 with 15-17 floating points, whereas Snowflake returns integers of 17-19 digits.
   * @param seed Seed to use in the random function.
   * @return Random number.
   */
  def randn(seed: Long): Column =
    builtin("RANDOM")(seed)

  /**
   * Wrapper for Spark json_tuple. This leverages JSON_EXTRACT_PATH_TEXT and improves functionality by allowing multiple columns
   * in a single call, whereas JSON_EXTRACT_PATH_TEXT must be called once for every column.
   *
   * There were differences found between Spark json_tuple and this function:
   *
   * - Float type: Spark returns only 6 floating points, whereas Snowflake returns more.
   * - Timestamp type: Spark interprets input date as UTC and transforms to local timestamp, whereas Spark leaves the timestamp as-is.
   * - Complex JSON path expressions: This function allows the retrieval of values within json objects, whereas Spark only allows values from the root.
   * - Identifiers with spaces: Snowflake returns error when an invalid expression is sent, whereas Spark returns null.
   *
   * Usage:
   * {{{
   * df = session.createDataFrame(Seq(("CR", "{\"id\": 5, \"name\": \"Jose\", \"age\": 29}"))).toDF(Seq("nationality", "json_string"))
   * }}}
   * When the result of this function is the only part of the select statement, no changes are needed:
   * {{{
   * df.select(json_tuple(col("json_string"), "id", "name", "age")).show()
   * }}}
   * ```
   * ----------------------
   * |"C0"  |"C1"  |"C2"  |
   * ----------------------
   * |5     |Jose  |29    |
   * ----------------------
   * ```
   * However, when specifying multiple columns, an expression like this is required:
   * {{{
   * df.select(
   *   col("nationality")
   *   , json_tuple(col("json_string"), "id", "name", "age"):_* // Notice the :_* syntax.
   * ).show()
   * }}}
   * ```
   * -------------------------------------------------
   * |"NATIONALITY"  |"C0"  |"C1"  |"C2"  |"C3"      |
   * -------------------------------------------------
   * |CR             |5     |Jose  |29    |Mobilize  |
   * -------------------------------------------------
   * ```
   * @param json Column containing the JSON string text.
   * @param fields Fields to pull from the JSON file.
   * @return Column sequence with the specified strings.
   */
  def json_tuple(json: Column, fields: String*): Seq[Column] = {
    var i = -1
    fields.map(f => {
        i += 1
        builtin("JSON_EXTRACT_PATH_TEXT")(json, f).as(s"c$i")
      }
    )
  }

  /**
   * Wrapper for Spark CBRT(Column) function. Used to calculate the cubic root of a number. There were slight differences found:
   * cbrt(341) -> Spark: 6.986368027818106, Snowflake: 6.986368027818107 (Notice the last decimal).
   * cbrt(2147483647) -> Spark: 1290.159154892091, Snowflake: 1290.1591548920912 (Notice the difference at the end).
   * This difference is acceptable.
   * @param column Column to calculate the cubic root.
   * @return Column object.
   */
  def cbrt(e: Column): Column = {
    builtin("CBRT")(e)
  }

  /**
   * Wrapper for Spark CBRT(String) function. Used to calculate the cubic root of a number. There were slight differences found:
   * cbrt(341) -> Spark: 6.986368027818106, Snowflake: 6.986368027818107 (Notice the last decimal).
   * cbrt(2147483647) -> Spark: 1290.159154892091, Snowflake: 1290.1591548920912 (Notice the difference at the end).
   * This difference is acceptable.
   * @param column Column to calculate the cubic root.
   * @return Column object.
   */
  def cbrt(columnName: String): Column = {
    cbrt(col(columnName))
  }

  /**
   * Wrapper for Spark `from_json` column function. This function converts a JSON string to a struct in Spark (variant in Snowflake).
   * Spark has several overloads for this function, where you specify the schema in which to convert it to the desired names and datatypes.
   *
   * Since we are using `TRY_PARSE_JSON` to replicate the functionality, we're not able to specify the structure. Snowflake will automatically parse everything.
   * However, it parses everything as strings. Manual casts need to be performed, for example:
   *
   * There were differences found between Spark from_json and this function when performing a select on the resulting column:
   *  - Float type: Spark returns only 7 floating points, whereas Snowflake returns more.
   *  - Timestamp type: Spark interprets input date as UTC and transforms to local timestamp, whereas Spark leaves the timestamp as-is.
   *  - Spark allows the selection of values within the resulting object by navigating through the object with a '.' notation. For example: df.select("json.relative.age")
   * On Snowflake, however this does not work. It is required to use df.selectExprs function, and the same example should be translated to
   * "json['relative']['age']" to access the value.
   *  - Since Spark receives a schema definition for the JSON string to read, it reads the values from the JSON and converts it to the specified data type.
   * On Snowflake the values are converted automatically, however they're converted as variants, meaning that the printSchema function would return different datatypes.
   * To convert the datatype and it to be printed as the expected datatype, it should be read on the selectExpr function as "json['relative']['age']::integer".
   * {{{
   * val data_for_json = Seq(
   *   (1, "{\"id\": 172319, \"age\": 41, \"relative\": {\"id\": 885471, \"age\": 29}}"),
   *   (2, "{\"id\": 532161, \"age\": 17, \"relative\":{\"id\": 873513, \"age\": 47}}")
   * )
   * val data_for_json_column = Seq("col1", "col2")
   * val df_for_json = session.createDataFrame(data_for_json).toDF(data_for_json_column)
   *
   * val json_df = df_for_json.select(
   *   from_json(col("col2")).as("json")
   * )
   *
   * json_df.selectExpr(
   *   "json['id']::integer as id"
   *   , "json['age']::integer as age"
   *   , "json['relative']['id']::integer as rel_id"
   *   , "json['relative']['age']::integer as rel_age"
   * ).show(10, 10000)
   * }}}
   * ```
   * -----------------------------------------
   * |"ID"    |"AGE"  |"REL_ID"  |"REL_AGE"  |
   * -----------------------------------------
   * |172319  |41     |885471    |29         |
   * |532161  |17     |873513    |47         |
   * -----------------------------------------
   * ```
   * @param e String column to convert to variant.
   * @return Column object.
   */
  def from_json(e: Column): Column = {
    builtin("TRY_PARSE_JSON")(e)
  }

  /**
   * Implementation for Spark date_sub. This function receives a date or timestamp, as well as a properly formatted string and subtracts the specified
   * amount of days from it. If receiving a string, this string is casted to date using try_cast and if it's not possible to cast, returns null. If receiving
   * a timestamp it will be casted to date (removing its time).
   *
   * There are some functional differences with this function:
   *  - Snowflake infers timestamp format better than Spark when the column is a string. For example, Spark returns null when specifying:
   *  `05/15/2021 06:54:34` returns null whereas Snowflake returns the expected value.
   * @param start Date, Timestamp or String column to subtract days from.
   * @param days Days to subtract.
   * @return Column object.
   */
  def date_sub(start: Column, days: Int): Column = {
    dateadd("DAY", lit(days * -1), sqlExpr(s"try_cast(${start.getName.get} :: STRING as DATE)"))
  }

  /**
   * Implementation for Spark bround. This function receives a column with a number and rounds it to scale decimal places
   * with HALF_EVEN round mode, often called as "Banker's rounding" . This means that if the number is at the same distance
   * from an even or odd number, it will round to the even number.
   *
   * @param colName Column to round.
   * @param scale Number of decimals to preserve.
   * @return Rounded number.
   */
  def bround(colName: Column, scale: Int): Column = {
    val power = pow(lit(10), lit(scale))
    val elevatedColumn = when(lit(0) === lit(scale), colName).otherwise(colName * power)
    val columnFloor = floor(elevatedColumn)
    when(
      elevatedColumn - columnFloor === lit(0.5)
      , when(columnFloor % lit(2) === lit(0), columnFloor).otherwise(columnFloor + lit(1))
    ).otherwise(round(elevatedColumn)) / when(lit(0) === lit(scale), lit(1)).otherwise(power)
  }

  /**
   * Implementation for Spark bround. This function receives a column and rounds it to 0 decimals  with HALF_EVEN round
   * mode, often called as "Banker's rounding" . This means that if the number is at the same distance from an even or
   * odd number, it will round to the even number.
   *
   * @param colName Column to round.
   * @param scale Number of decimals to preserve.
   * @return Rounded number.
   */
  def bround(colName: Column): Column = {
    bround(colName, 0)
  }

  /**
   * Implementation for Spark `regexp_extract`. This function receives a column and extracts the groupIdx from the string
   * after applying the exp regex. Spark returns empty string when the string doesn't match and null if the input is null.
   * This behavior has been replicated, since Snowflake returned null on both cases.
   *
   * This function applies the `case sensitive` and `extract` flags. It doesn't apply multiline nor .* matches newlines.
   * Even though Snowflake is capable of doing so with the `regexp_substr` function, Spark's behavior for this function
   * doesn't allow this. If these flags need to be applied, use `builtin("REGEXP_SUBSTR")` instead and apply the desired
   * flags.
   *
   * Note: This function ensures that the function has the same functionality on Snowflake as it does on Spark. However,
   * it doesn't guarantee that regular expressions on Spark will work the same on Snowflake. They still have different
   * engines and the regex itself needs to be analyzed manually. For example: non-greedy tokens such as `.*?` don't work
   * on Snowflake as they do on Spark.
   *
   * @param colName Column to apply regex.
   * @param exp Regex expression to apply.
   * @param grpIdx Group to extract.
   * @return Column object.
   */
  def regexp_extract(colName: Column, exp: String, grpIdx: Int): Column = {
    when(colName.is_null, lit(null))
      .otherwise(
        coalesce(
          builtin("REGEXP_SUBSTR")(colName, lit(exp), lit(1), lit(1), lit("ce"), lit(grpIdx)), lit("")
        )
      )
  }

  /**
   * Wrapper for Spark signum function. Returns the sign of the given column. Returns either 1 for positive, 0 for 0 or
   * NaN, -1 for negative and null for null.
   *
   * There are some functional differences with Spark:
   *  - Spark returns NaN when NaN is provided.
   *  - Spark and Snowflake can receive string and attempts to cast. If it casts correctly, returns the calculation,
   *  if not returns Spark returns null whereas Snowflake returns error.
   *
   * @param e Column to calculate the sign.
   * @return Column object.
   */
  def signum(colName: Column): Column = {
    builtin("SIGN")(colName)
  }

  /**
   * Wrapper for Spark signum function. Returns the sign of the given column. Returns either 1 for positive, 0 for 0 or
   * NaN -1 for negative and null for null.
   *
   * There are some functional differences with Spark:
   *  - Spark returns NaN when NaN is provided.
   *  - Spark and Snowflake can receive string and attempts to cast. If it casts correctly, returns the calculation,
   *  if not returns Spark returns null whereas Snowflake returns error.
   *
   * @param columnName Name of the column to calculate the sign.
   * @return Column object.
   */
  def signum(columnName: String): Column = {
    signum(col(columnName))
  }


  ///////////////////////////////////// Tannia ////////////////////////////////////////////////


  ///////////////////////////////////// Fonse ////////////////////////////////////////////////

  /**
   * Wrapper for Snowflake built-in array function. Create array from columns names.
   * @param s Columns names to build the array.
   * @return The array.
   */
  def array(colName: String, colNames: String*) = array_construct((colName +: colNames).map(col) : _*)

  /**
   * Wrapper for Snowflake built-in collect_list function. Get the values of array column.
   * @param c Column to be collect.
   * @return The array.
   */
  def collect_list(c: Column) = array_agg(c)

  /**
   * Wrapper for Snowflake built-in collect_list function. Get the values of array column.
   * @param s Column name to be collected.
   * @return The array.
   */
  def collect_list(s: String) = array_agg(col(s))

  /**
   * Wrapper for Snowflake built-in reverse function. Gets the reversed string.
   * @param c Column to be reverse.
   * @return Column object.
   */
  def reverse(c: Column) =
    builtin("reverse")(c)

  /**
   * Wrapper for Snowflake built-in isnull function. Gets a boolean depending if value is NULL or not.
   * @param c Column to qnalize if it is null value.
   * @return Column object.
   */
  def isnull(c: Column) = is_null(c)

  /**
   * Wrapper for Snowflake built-in last function. Gets the last value of a column according to its grouping.
   * @param c Column to obtain last value.
   * @return Column object.
   */
  def last(s: String) =
    builtin("LAST_VALUE")(col(s))

  /**
   * Wrapper for Snowflake built-in conv function. Convert number with from and to base.
   * @param c Column to be converted.
   * @param fromBase Column from base format.
   * @param toBase Column to base format.
   * @return Column object.
   */
  def conv(c: Column, fromBase: Int, toBase: Int) = callBuiltin("conv", c, fromBase,toBase)

  /**
   * Wrapper for Snowflake built-in last function. Gets the last value of a column according to its grouping.
   * Functional difference with windows, In Snowpark is needed the order by. SQL doesn't guarantee the order.
   * @param s Column name to get last value.
   * @param nulls Consider null values or not.
   * @return Column object.
   */
  def last(s: String, nulls: Boolean) = {
    if (nulls){
      sqlExpr(s"LAST_VALUE(${col(s).getName.get}) IGNORE NULLS")
    } else {
      sqlExpr(s"LAST_VALUE(${col(s).getName.get}) RESPECT NULLS")
    }
  }

  /**
   * Wrapper for Snowflake built-in last function. Gets the last value of a column according to its grouping.
   * Functional difference with windows, In Snowpark is needed the order by. SQL doesn't guarantee the order.
   * @param c Column to get last value.
   * @param nulls Consider null values or not.
   * @return Column object.
   */
  def last(c: Column, nulls: Boolean) = {
    if (nulls){
      sqlExpr(s"LAST_VALUE(${c.getName.get}) IGNORE NULLS")
    } else {
      sqlExpr(s"LAST_VALUE(${c.getName.get}) RESPECT NULLS")
    }
  }

  /**
   * Wrapper for Snowflake built-in first function. Gets the first value of a column according to its grouping.
   * @param c Column to obtain first value.
   * @return Column object.
   */
  def first(s: String) =
    builtin("FIRST_VALUE")(col(s))

  /**
   * Wrapper for Snowflake built-in first function. Gets the first value of a column according to its grouping.
   * @param s Column name to get first value.
   * @param nulls Consider null values or not.
   * @return Column object.
   */
  def first(s: String, nulls: Boolean) = {
    if (nulls){
      sqlExpr(s"FIRST_VALUE(${col(s).getName.get}) IGNORE NULLS")
    } else {
      sqlExpr(s"FIRST_VALUE(${col(s).getName.get}) RESPECT NULLS")
    }
  }

  /**
   * Wrapper for Snowflake built-in last function. Gets the last value of a column according to its grouping.
   * @param c Column to get last value.
   * @param nulls Consider null values or not.
   * @return Column object.
   */
  def first(c: Column, nulls: Boolean) = {
    if (nulls){
      sqlExpr(s"FIRST_VALUE(${c.getName.get}) IGNORE NULLS")
    } else {
      sqlExpr(s"FIRST_VALUE(${c.getName.get}) RESPECT NULLS")
    }
  }

  ///////////////////////////////////// Jeremy ////////////////////////////////////////////////

  /**
   * Returns the current Unix timestamp (in seconds) as a long.
   *
   * @note All calls of `unix_timestamp` within the same query return the same value
   */
  def unix_timestamp(): Column = {
    builtin("date_part")("epoch_second", current_timestamp())
  }

  /**
   * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
   * using the default timezone and the default locale.
   *
   * @param s A date, timestamp or string. If a string, the data must be in the
   *          `yyyy-MM-dd HH:mm:ss` format
   * @return A long, or null if the input was a string not of the correct format
   */
  def unix_timestamp(s: Column): Column = {
    builtin("date_part")("epoch_second", s)
  }

  /**
   * Converts time string with given pattern to Unix timestamp (in seconds).
   *
   * @param s A date, timestamp or string. If a string, the data must be in a format that can be
   *          cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param p A date time pattern detailing the format of `s` when `s` is a string
   * @return A long, or null if `s` was a string that could not be cast to a date or `p` was
   *         an invalid format
   */
  def unix_timestamp(s: Column, p: String): Column = {
    builtin("date_part")("epoch_second", to_timestamp(s, lit(p)))
  }

  /**
   * Wrapper for Snowflake built-in regexp_replace function. Replaces parts of a string with the specified replacement value, based on a regular expression.
   * @param strExpr String to apply replacement.
   * @param pattern Regex pattern to find in the expression.
   * @param replacement Column to replace within the string.
   * @return Column object.
   */
  def regexp_replace(strExpr: Column, pattern: Column, replacement: Column): Column =
    builtin("regexp_replace")(strExpr, pattern, replacement)

  /**
   * Wrapper for Snowflake built-in regexp_replace function. Replaces parts of a string with the specified replacement value, based on a regular expression.
   * @param strExpr String to apply replacement.
   * @param pattern Regex pattern to find in the expression.
   * @param replacement Column to replace within the string.
   * @return Column object.
   */
  def regexp_replace(strExpr: Column, pattern: String, replacement: String): Column = {
    builtin("regexp_replace")(strExpr, pattern, replacement)
  }

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  The number of days to add to `start`, can be negative to subtract days
   * @return A date, or null if `start` was a string that could not be cast to a date
   */
  def date_add(start: Column, days: Int): Column = dateadd("day", lit(days), start)

  /**
   * Returns the date that is `days` days after `start`
   *
   * @param start A date, timestamp or string. If a string, the data must be in a format that
   *              can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param days  The number of days to add to `start`, can be negative to subtract days
   * @return A date, or null if `start` was a string that could not be cast to a date
   */
  def date_add(start: Column, days: Column): Column = dateadd("day", days, start)

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @param e The column to collect the list values
   * @return A list with unique values
   */
  def collect_set(e: Column): Column = sqlExpr(s"array_agg(distinct ${e.getName.get})")

  /**
   * Aggregate function: returns a set of objects with duplicate elements eliminated.
   *
   * @param e The column to collect the list values
   * @return A list with unique values
   */
  def collect_set(e: String): Column = sqlExpr(s"array_agg(distinct ${e})")

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the
   * yyyy-MM-dd HH:mm:ss format.
   *
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @return A string, or null if the input was a string that could not be cast to a long
   */
  def from_unixtime(ut: Column): Column =
    ut.cast(LongType).cast(TimestampType).cast(StringType)

  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   *
   * @param ut A number of a type that is castable to a long, such as string or integer. Can be
   *           negative for timestamps before the unix epoch
   * @param f  A date time pattern that the input will be formatted to
   * @return A string, or null if `ut` was a string that could not be cast to a long or `f` was
   *         an invalid date time pattern
   */
  def from_unixtime(ut: Column, f: String): Column =
    date_format(ut.cast(LongType).cast(TimestampType), f)

  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   */
  def monotonically_increasing_id(): Column = builtin("seq8")()

  /**
   * Returns number of months between dates `start` and `end`.
   *
   * A whole number is returned if both inputs have the same day of month or both are the last day
   * of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
   *
   * For example:
   * {{{
   * months_between("2017-11-14", "2017-07-14")  // returns 4.0
   * months_between("2017-01-01", "2017-01-10")  // returns 0.29032258
   * months_between("2017-06-01", "2017-06-16 12:00:00")  // returns -0.5
   * }}}
   *
   * @param end   A date, timestamp or string. If a string, the data must be in a format that can
   *              be cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param start A date, timestamp or string. If a string, the data must be in a format that can
   *              cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @return A double, or null if either `end` or `start` were strings that could not be cast to a
   *         timestamp. Negative if `end` is before `start`
   */
  def months_between(end: Column, start: Column): Column = builtin("MONTHS_BETWEEN")(start, end)

  /**
   * Locate the position of the first occurrence of substr column in the given string.
   * Returns null if either of the arguments are null.
   *
   * @note The position is not zero based, but 1 based index. Returns 0 if substr
   * could not be found in str.
   */
  def instr(str: Column, substring: String): Column = builtin("REGEXP_INSTR")(str,substring)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def from_utc_timestamp(ts: Column, tz: String): Column =
    builtin("TO_TIMESTAMP_TZ")(ts,tz)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC, and renders
   * that time as a timestamp in the given time zone. For example, 'GMT+1' would yield
   * '2017-07-14 03:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def from_utc_timestamp(ts: Column, tz: Column): Column =
    builtin("TO_TIMESTAMP_TZ")(ts,tz)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def to_utc_timestamp(ts: Column, tz: String): Column = builtin("TO_TIMESTAMP_TZ")(ts,tz)

  /**
   * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in the given time
   * zone, and renders that time as a timestamp in UTC. For example, 'GMT+1' would yield
   * '2017-07-14 01:40:00.0'.
   *
   * @param ts A date, timestamp or string. If a string, the data must be in a format that can be
   *           cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
   * @param tz A string detailing the time zone ID that the input should be adjusted to. It should
   *           be in the format of either region-based zone IDs or zone offsets. Region IDs must
   *           have the form 'area/city', such as 'America/Los_Angeles'. Zone offsets must be in
   *           the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are
   *           supported as aliases of '+00:00'. Other short names are not recommended to use
   *           because they can be ambiguous.
   * @return A timestamp, or null if `ts` was a string that could not be cast to a timestamp or
   *         `tz` was an invalid value
   */
  def to_utc_timestamp(ts: Column, tz: Column): Column = builtin("TO_TIMESTAMP_TZ")(ts,tz)

  /**
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places
   * with HALF_EVEN round mode, and returns the result as a string column.
   *
   * If d is 0, the result has no decimal point or fractional part.
   * If d is less than 0, the result will be null.
   *
   * @param x numeric column to be transformed
   * @param d Amount of decimal for the number format
   *
   * @return Number casted to the specific string format
   */
  def format_number(x: Column, d: Int): Column = {
    if(d < 0){
     lit(null)
    } else {
      builtin("TO_VARCHAR")(x, if (d > 0) s"999,999.${"0" * d}" else "999,999")
    }
  }

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * @param expr Column to apply this mathematical operation
   *
   * @return log2 of the given column
   */
  def log2(expr: Column): Column = builtin("LOG")(2,expr)

  /**
   * Computes the logarithm of the given column in base 2.
   *
   * @param columnName Column to apply this mathematical operation
   *
   * @return log2 of the given column
   */
  def log2(columnName: String): Column = builtin("LOG")(2,col(columnName))



}

}

