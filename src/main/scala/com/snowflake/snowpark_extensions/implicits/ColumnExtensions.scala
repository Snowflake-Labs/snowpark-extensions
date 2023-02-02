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

import com.snowflake.snowpark.Column
import com.snowflake.snowpark.functions.{builtin, lit, when, sqlExpr, substring}
import net.snowflake.client.jdbc.internal.apache.tika.metadata.Metadata
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata
import net.snowflake.client.jdbc.internal.apache.arrow.flatbuf.Bool
import com.snowflake.snowpark.types._

/** Column Extensions object containing implicit functions to the Snowpark Column object. */
object ColumnExtensions {

  /**
   * Column extension class.
   * @param c Column to extend functionality.
   */
  class ExtendedColumn(c: Column) {

    /**
     * Function that validates if the value of the column is within the list of strings from parameter.
     * @param strings List of strings to compare with the value.
     * @return Column object.
     */
    def isin(strings: String*): Column = {
      c.in(strings)
    }

    /**
     * Function that validates if the value of the column is not within the list of strings from parameter.
     * @param strings List of strings to compare with the value.
     * @return Column object.
     */
    def not_isin(strings: String*): Column = {
      !c.in(strings)
    }

    /**
     * Function that validates if the values of the column are not null.
     * @return Column object.
     */
    def isNotNull() =
      c.is_not_null

    /**
     * Function that validates if the values of the column are null.
     * @return Column object.
     */
    def isNull() =
      c.is_null

    /**
     * Function that validates if the values of the column start with the parameter expression.
     * @param expr Expression to validate with the column's values.
     * @return Column object.
     */
    def startsWith(expr: String) =
      com.snowflake.snowpark.functions.startswith(c,lit(expr))

    /**
     * Function that validates if the values of the column contain the value from the paratemer expression.
     * @param expr Expression to validate with the column's values.
     * @return Column object.
     */
    def contains(expr: String) =
      builtin("contains")(c, expr)

    /**
     * Function that replaces column's values according to the regex pattern and replacement value parameters.
     * @param pattern Regex pattern to replace.
     * @param replacement Value to replace matches with.
     * @return Column object.
     */
    def regexp_replace(pattern: String, replacement: String) =
      builtin("regexp_replace")(c, pattern, replacement)

    /**
     * Function that gives the column an alias using a symbol.
     * @param symbol Symbol name.
     * @return Column object.
     */
    def as(symbol: Symbol): Column =
      c.as(symbol.name)
    
    /**
     * Function that returns True if the current expression is NaN.
     * @return Column object.
     */
    def isNaN(): Column = 
      when(sqlExpr(s"try_cast(${c.getName.get} :: STRING as FLOAT)").is_not_null, c.cast(FloatType).equal_nan).otherwise(lit(false))

    /**
     * Function that returns the portion of the string or binary value str, starting from the character/byte specified by pos, with limited length.
     * @param pos Start position.
     * @param len Length of the substring.
     * @return Column object.
     */
    def substr(pos: Column , len: Column): Column = 
      substring(c, pos, len)

    /**
     * Function that returns the portion of the string or binary value str, starting from the character/byte specified by pos, with limited length.
     * @param pos Start position.
     * @param len Length of the substring.
     * @return Column object.
     */
    def substr(pos: Int, len: Int): Column = 
      substring(c, lit(pos), lit(len))

    /**
     * Function that returns the result of the comparison of two columns.
     * @param other Column to compare.
     * @return Column object.
     */
    def notEqual(other: Any): Column =
      c.not_equal(lit(other))

    /**
     * Function that returns a boolean column based on a match.
     * @param literal expresion to match.
     * @return Column object.
     */
    def like(literal: String): Column =
      c.like(lit(literal))

    /**
     * Function that returns a boolean column based on a regex match.
     * @param literal Regex expresion to match.
     * @return Column object.
     */
    def rlike(literal: String): Column =
      c.regexp(lit(literal))

    /**
     * Function that computes bitwise AND of this expression with another expression.
     * @param other Expression to match.
     * @return Column object.
     */
    def bitwiseAND(other: Any): Column =
      c.bitand(lit(other))

    /**
     * Function that computes bitwise OR of this expression with another expression.
     * @param other Expression to match.
     * @return Column object.
     */
    def bitwiseOR(other: Any): Column =
      c.bitor(lit(other))

    /**
     * Function that computes bitwise XOR of this expression with another expression.
     * @param other Expression to match.
     * @return Column object.
     */
    def bitwiseXOR(other: Any): Column =
      c.bitxor(lit(other))

    /**
     * Function that gets an item at position ordinal out of an array, or gets a value by key in a object. 
     * Functional Difference with Spark: The function returns a Variant type.
     * @param key Key element to get.
     * @return Column object.
     */
    def getItem(key: Any): Column =
        builtin("get")(c, key)

    /**
     * Function that gets a value by field name or key in a object. 
     * Functional Difference with Spark: The function returns a Variant type.
     * This function works for StructType in Spark. The equivalent in Snowpark is object.
     * @param fieldName Field name.
     * @return Column object.
     */
    def getField(fieldName: String): Column =
        builtin("get")(c, fieldName)

    /**
     * Function that returns a boolean column based on a pattern match.
     * @param other Pattern to match.
     * @return Column object.
     */
    def contains(other: Any): Column =
        c.like(lit(other))

    /**
     * Function that casts the column to a different data type, using the canonical string representation of the type. 
     * The supported types are: string, boolean, byte, short, int, long, float, double, decimal, date, timestamp.
     * Functional Difference with Spark: Spark returns null when casting is not possible. Snowflake throws an exception. 
     * @param to String representation of the type.
     * @return Column object.
     */
    def cast(to: String): Column = {
      to match {
        case "string" => c.cast(StringType)
        case "boolean" => c.cast(BooleanType)
        case "byte" => c.cast(ByteType)
        case "short" => c.cast(ShortType)
        case "int" => c.cast(IntegerType)
        case "long" => c.cast(LongType)
        case "float" => c.cast(FloatType)
        case "double" => c.cast(DoubleType)
        case "decimal" => c.cast(DecimalType(38,0))
        case "date" => c.cast(DateType)
        case "timestamp" => c.cast(TimestampType)  
        case _ => lit(null)
      }
    }

    /**
     * Function that performs quality test that is safe for null values.
     * @param other Value to compare
     * @return Column object.
     */
    def eqNullSafe(other: Any): Column =
      c.equal_null(lit(other))
  }

}
