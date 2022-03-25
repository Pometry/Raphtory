`com.raphtory.core.algorithm.Row`
(com.raphtory.core.algorithm.Row)=
# Row

{s}`Row(values: Any*)`
   : Create a row of a data table

## Parameters

 {s}`values: Any*`
   : Values to store in row

## Methods

 {s}`apply(index: Int): Any`
   : Return value at `index`

 {s}`get(index: Int): Any`
   : same as `apply`

 {s}`getAs[T](index: Int): T`
     : Return value at `index` and cast it to type `T`

 {s}`getInt(index: Int): Int`
   : Same as {s}`getAs[Int](index)`

 {s}`getString(index: Int): String`
   : Same as {s}`getAs[String](index)`

 {s}`getBool(index: Int): Boolean`
   : Same as {s}`getAs[Boolean](index)`

 {s}`getLong(index: Int): Long`
   : Same as {s}`getAs[Long](index)`

 {s}`getDouble(index: Int): Double`
   : Same as {s}`getAs[Double](index)`

 {s}`getValues(): Array[Any]`
   : Return Array of values

 ```{seealso}
 [](com.raphtory.core.algorithm.Table)
 ```