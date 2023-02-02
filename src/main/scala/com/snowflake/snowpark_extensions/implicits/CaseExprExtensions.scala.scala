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

    /**
     * Evaluates a list of conditions and returns one of multiple possible result expressions. If otherwise is not defined at the end, null is returned for unmatched conditions.
     * @param value value of Any to apli.
     * @return Column object.
     */
    def otherwise(value: Any): Column = {
      c.otherwise(lit(value))
    }
  }

}