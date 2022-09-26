package com.snowflake.snowpark_extensions.testutils

import java.text.SimpleDateFormat

 // import java.util.Date
  //import java.text.SimpleDateFormat

  
object DataFrameCreator {

  val format = new SimpleDateFormat("yyyy-MM-dd") 
  def todate(x:String) = new java.sql.Date(format.parse(x).getTime)
  // val data_for_ntile = Seq(
  // (101,10.01, todate("2021-01-01")),
  // (101,102.01, todate("2021-01-01")),
  // (102,93.0, todate("2021-01-01")),
  // (103,913.1, todate("2021-01-02")),
  // (102,913.1, todate("2021-01-02")),
  // (101,900.56, todate("2021-01-03")))


  val data_for_ntile = Seq(
  (101,10.01, 1),
  (101,102.01, 1),
  (102,93.0, 1),
  (103,913.1, 2),
  (102,913.1, 2),
  (101,900.56, 3))  
  val data_for_ntile_columns = Seq("ACCT","AMT", "TXN_DT")

  val data_for_general = Seq((1, 1.1, null,"c"), (2, 2.1, "two", "c"), (237, 237.1, "two hundred thirty seven", "g"))
  val data_for_general_column = Seq("col1", "col2", "col3", "col4")
  val data_for_float = Seq((0.1f,1, "test123", 5), (Float.NaN,5, "test", 5))
  val data_for_float_column = Seq("col1", "col2", "col3", "col4")
  val data_for_base64 = Seq(("Jules Newone",1), ("Mr Ronald M ",2))
  val data_for_base64_column = Seq("col1","col2")
  val data_for_double = Seq((0.1d,1, "test123", 5), (Double.NaN,5, "test", 5))
  val data_for_double_column = Seq("col1", "col2", "col3", "col4")
  val data_for_double2 = Seq((0.1d,1, "test123", 5))
  val data_for_double2_column = Seq("col1", "col2", "col3", "col4")
  val data_for_array = Seq((Array("a","b","c"), 2), (Array("a"), 4), (Array(""),6))
  val data_for_array_column = Seq("col1", "col2")
  val data_for_cast = Seq(("true",1,"123","2.1","2021-05-15 06:54:34"), ("false",0,"15","5.4","2021-05-15"), (null,0,"0","0.0",null))
  val data_for_cast_column = Seq("col1", "col2", "col3", "col4", "col5")
  val data_for_order = Seq((1,"a"), (5,"e"), (9,"i"), (3,"c"), (2,"b"), (7,"g"), (6,"f"), (5,null), (4,"d"), (8,"h"))
  val data_for_order_column = Seq("col1", "col2")
  val data_for_date_cast = Seq((1, 1.1, null,"c", null, null), (2, 2.1, "two", "c", "2021-05-15", "05/15/2021 06:54:34"), (2, 2.1, "two", "c", "2022-08-01", "08/01/2022 14:12:16"))
  val data_for_date_cast_column = Seq("col1", "col2", "col3", "col4", "col5", "col6")
  val data_for_json = Seq(
    ("company1", 125, Array(Array("a", "b", "c"), Array("1","2","3")), "{\"id\": 172319, \"age\": 41, \"relative\": {\"id\": 885471, \"age\": 29}}"),
    ("company2", 341, Array(Array("d", "e", "f"), Array("4","5","6")), "{\"id\": 532161, \"age\": 17, \"relative\":{\"id\": 873513, \"age\": 47}}"),
    ("company3",  81, Array(Array("g", "h", "i"), Array("7","8","9")), "{\"id\": 761323, \"age\": 81, \"relative\":{\"id\": 123286, \"age\":  5}}"),
    ("company4", 990, Array(Array("j", "k", "l"), Array("10","11","12")), "{\"id\": 831385, \"age\": 13, \"relative\":{\"id\": 584321, \"age\": 37}}"),
    ("company5", 2147483647, Array(Array("m", "n", "o"), Array("13","14","15")), "{\"id\": 984032, \"age\": 25, \"relative\": {\"id\": 246874, \"age\": 36}}"),
    ("company6", -4, Array(Array("p", "q", "r"), Array("16","17","18")), "{\"id\": 986131, \"age\": 3, \"relative\": {\"id\": 246874, \"age\": 69}}"),
    ("company7", -135, Array(Array("s", "t", "u"), Array("19","20","21")), "{\"id\": 873513, \"age\": 62,\"relative\": {\"id\": 246874, \"age\": 72}")
  )
  val data_for_json_column = Seq("col1", "col2", "col3", "col4")
  val data_for_json_complex = Seq(
    (1, "{\"int\": 3248321, \"float\": 5.87613255, \"boolean\": true, \"empty\": null, \"str\": \"Some text over here\", \"long\": 214748364786, \"dbl\": 214748364777.4843132135, \"time\": \"14:55:22\", \"date\": \"2012-03-19\", \"datetime\": \"2017-05-22T07:22:14Z\", \"relative\": {\"age\": 25}}")
  )
  val data_for_json_complex_column = Seq("col1", "col2")
  val data_for_union_all = Seq((1, 1.1, null,"c"), (3, 3.2, "three", "d"), (4, 4.3, "four", "e"))
  val data_for_union_all_column = Seq("col1", "col2", "col3", "col4")
  val data_for_date_format = Seq((null, null), ("2021-05-15", "2021-05-15 06:54:34"), ("2022-08-01", "2022-08-01 14:12:16"))
  val data_for_date_format_column = Seq("col1", "col2")
  val data_for_rounding = Seq((1.5d,-1.5d), (1.6d,-1.6d), (1.45d,-1.45d), (1.45d,-1.45d), (6.5d,-6.5d), (12.45d,-12.45d), (25.15d,-21.15d), (17.14d,-17.14d), (18.14d,-18.14d), (18.22d,-18.22d), (15.255d, -15.255d), (0.0d, Double.NaN))
  val data_for_rounding_column = Seq("col1", "col2")
  val data_for_regex = Seq(("This is some text to test. Some tests are simple.","Testing is nice. That is a test."),("Something here",null))
  val data_for_regex_column = Seq("col1", "col2")
  val data_for_signum = Seq((45.0d, -45.0d), (0.0d, -7.0d), (17.0d, -17.0d))
  val data_for_signum_column = Seq("col1", "col2")
  val data_for_conv = Seq(("10101","a"), ("10001","e"), ("110111","i"), ("11101","c"))
  val data_for_conv_column = Seq("col1", "col2")
  val data_for_window = Seq((1, 10), (1, 11), (1, 12), (2, 20), (2, 21), (2, 22))
  val data_for_window_column = Seq("col1", "col2")

}
