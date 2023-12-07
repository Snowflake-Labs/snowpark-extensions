package com.snowflake.snowpark_extensions.implicits

import com.snowflake.snowpark.Column
import com.snowflake.snowpark.CaseExpr
import com.snowflake.snowpark.functions.{builtin, lit, when, sqlExpr, substring}
import net.snowflake.client.jdbc.internal.apache.tika.metadata.Metadata
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata
import net.snowflake.client.jdbc.internal.apache.arrow.flatbuf.Bool
import com.snowflake.snowpark.types._

/** Column Extensions object containing implicit functions to the Snowpark Column object. */
object CaseExprExtensions {

  /**
   * CaseExpr extension class.
   * @param c CaseExpr to extend functionality.
   */
  class ExtendedCaseExpr(c: CaseExpr) {
    def toCol() = {
      c
    }
    /**
     * Appends one more WHEN condition to the CASE expression.
     * @param value value of Any to apply
     * @return Column object.
     */
    def when( condition: Column , value: Any ) : ExtendedCaseExpr = {
      new ExtendedCaseExpr(c.when(condition,lit(value)))
    }
    /**
     * Evaluates a list of conditions and returns one of multiple possible result expressions. If otherwise is not defined at the end, null is returned for unmatched conditions.
     * @param value value of Any to apply
     * @return Column object.
     */
    def otherwise(value: Any): Column = {
      c.otherwise(lit(value))
    }
  }

}