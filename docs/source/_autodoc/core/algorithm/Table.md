`com.raphtory.core.algorithm.Table`
(com.raphtory.core.algorithm.Table)=
# Table

{s}`Table`
 : Interface for table operations

## Methods

 {s}`filter(f: Row => Boolean): Table`
   : add a filter operation to table

     {s}`f: Row => Boolean`
       : function that runs once for each row (only rows for which {s}`f ` returns {s}`true` are kept)

 {s}`explode(f: Row => List[Row]): Table`
   : add an explode operation to table. This creates a new table where each row in the old table
     is mapped to multiple rows in the new table.

     {s}`f: Row => List[Row]`
       : function that runs once for each row and returns a list of new rows

 {s}`writeTo(outputFormat: OutputFormat)`
   : write out data based on [{s}`outputFormat`](com.raphtory.core.algorithm.OutputFormat)

 ```{seealso}
 [](com.raphtory.core.algorithm.Row), [](com.raphtory.core.algorithm.OutputFormat)
 ```