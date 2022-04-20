from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.java_gateway import java_import
from py4j.java_collections import MapConverter

gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True))

print("Importing items")
java_import(gateway.jvm, "com.raphtory.deployment.Raphtory")
java_import(gateway.jvm, "scala.collection.JavaConverters")
java_import(gateway.jvm, "com.raphtory.examples.lotrTopic.PythonUtils")
java_import(gateway.jvm, "com.raphtory.algorithms.generic.ConnectedComponents")
java_import(gateway.jvm, "com.raphtory.algorithms.generic.EdgeList")
java_import(gateway.jvm, "com.raphtory.output.FileOutputFormat")
java_import(gateway.jvm, "scala.immutable")
java_import(gateway.jvm, "com.raphtory.util.PythonUtils")

print("Setting up the Raphtory Client")
raphtory = gateway.jvm.Raphtory
customConfig = {"raphtory.deploy.id": "raphtory_1974606656", "raphtory.deploy.distributed": True}
mc_run_map_dict = MapConverter().convert(customConfig, gateway._gateway_client)
jmap = gateway.jvm.PythonUtils.toScalaMap(mc_run_map_dict)
client = raphtory.createClient(jmap)

# |  pointQuery(GraphAlgorithm, OutputFormat, long, List) : QueryProgressTracker
# |  pointQuery(GraphAlgorithm, OutputFormat, long) : QueryProgressTracker