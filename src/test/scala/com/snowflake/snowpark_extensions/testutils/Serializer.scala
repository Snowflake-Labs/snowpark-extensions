package com.snowflake.snowpark_extensions.testutils

object Serializer {
  def df2Seq(df : com.snowflake.snowpark.DataFrame): Seq[Seq[Any]] ={
    df.collect().map(_.toSeq)
  }

  def snowList2Seq(list: java.util.List[com.snowflake.snowpark.Row]): Array[Seq[Any]] = {
    list.toArray().map(f => {
      val r = f.asInstanceOf[com.snowflake.snowpark.Row]
      r.toSeq
    })
  }

}
