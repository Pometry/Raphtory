`com.raphtory.core.algorithm.OutputFormat`
(com.raphtory.core.algorithm.OutputFormat)=
# OutputFormat

{s}`OutputFormat`
   : Interface for output formats

 ## Attributes

   {s}`logger`
     : Logger instance for writing debug messages

 Concrete implementations need to override the `write` method to output the data.

 ## Methods

   {s}`write(timestamp: Long, window: Option[Long], jobID: String, row: Row, partitionID: Int): Unit`
     : Write out tabular data

       {s}`timestamp: Long`
          : timestamp for current graph perspective

       {s}`window: Option[Long]`
          : window of current perspective (if set)

       {s}`jobID: String`
          : ID of job that generated the data

       {s}`row: Row`
          : row of data to write out

       {s}`partitionID: Int`
          : ID of partition trying to write the data

 ```{seealso}
 [](com.raphtory.output.FileOutputFormat), [](com.raphtory.output.PulsarOutputFormat),
 [](com.raphtory.core.algorithm.Row)
 ```