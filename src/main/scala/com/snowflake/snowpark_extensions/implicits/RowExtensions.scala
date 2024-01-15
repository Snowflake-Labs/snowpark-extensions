package com.snowflake.snowpark_extensions.implicits
import com.snowflake.snowpark.Row

/** Row Extensions object containing implicit functions to the Snowpark Row object. */
object RowExtensions {
 class ExtendedRow(r: Row) {

    @deprecated("this method is not supported. It was added to avoid compilation issues", "SnowparkExtensions")
    def fieldIndex(name: String) : Int = 0
 }

}