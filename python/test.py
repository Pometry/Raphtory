import time
import raphtoryclient
raphtory = raphtoryclient.client(raphtory_deployment_id="raphtory_833062781")

raphtory.java_import("com.raphtory.algorithms.generic.EdgeList")
ConnectedComponents = raphtory.java().EdgeList

raphtory.java_import("com.raphtory.sinks.FileSink")
raphtory.java_import("com.raphtory.formats.CsvFormat")
CsvFormat = raphtory.java().CsvFormat
output = raphtory.java().FileSink.apply("/tmp/raphtoryEdgeList", CsvFormat())

queryHandler = raphtory.graph.at(32674).past().execute(ConnectedComponents()).writeTo(output)

time.sleep(25)