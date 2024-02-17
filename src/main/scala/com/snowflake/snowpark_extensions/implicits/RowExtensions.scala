package com.snowflake.snowpark_extensions.implicits
import com.snowflake.snowpark.Row
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.types.StructField


/** Row Extensions object containing implicit functions to the Snowpark Row object. */
object RowExtensions {
 import com.snowflake.snowpark.internal.analyzer
 class ExtendedRow(r: Row, schema:StructType = null) {

   def getAs[T](index: Int)(implicit tag: scala.reflect.ClassTag[T]): T = {
      val value = r.get(index)
      value match {
        case v: T => v
        case _ =>  throw new ClassCastException(s"Could not cast field from ${value.getClass().getSimpleName()} to ${tag.runtimeClass.getSimpleName}") 
      }
   }
  // Method to retrieve a field value by name from a Row and cast it to type T
  def getAs[T](fieldName: String)(implicit tag: scala.reflect.ClassTag[T]): T = {
    val index = fieldIndex(fieldName)
    if (index != -1) {
      getAs[T](index)
    } else
      throw new IllegalArgumentException(s"Field ${fieldName} not found")
   }

    def fieldIndex(name: String) : Int = 
      schema.fields.zipWithIndex.find 
      { 
         case (field, index) =>
            // Check if the field name matches the given fieldName
            field.columnIdentifier.quotedName == analyzer.quoteName(name)
      }.map(_._2).getOrElse(-1) // Extract the index from the Option
    def toRow() = { r }
 }
}