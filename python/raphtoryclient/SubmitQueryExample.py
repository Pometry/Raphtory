from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.java_gateway import java_import
gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True))

print("importing")
java_import(gateway.jvm, "com.raphtory.deployment.Raphtory")
java_import(gateway.jvm, "scala.collection.JavaConverters")
java_import(gateway.jvm, "com.raphtory.examples.lotrTopic.PythonUtils")
java_import(gateway.jvm, "com.raphtory.algorithms.generic.ConnectedComponents")
java_import(gateway.jvm, "com.raphtory.algorithms.generic.EdgeList")
java_import(gateway.jvm, "com.raphtory.output.FileOutputFormat")
java_import(gateway.jvm, "scala.immutable")

print("client")
raphtory = gateway.jvm.Raphtory
client = raphtory.createClient()

print("algo setup")
connectedComponentsAlgorithm = gateway.jvm.ConnectedComponents
edgeListAlgorithm = gateway.jvm.EdgeList
fileOutputFormat = gateway.jvm.FileOutputFormat
long_class = gateway.jvm.Long
long_array = gateway.new_array(long_class, 0)

print("pointquery cc ")
client.pointQuery(
    connectedComponentsAlgorithm(),
    fileOutputFormat.apply("/tmp/pythonCC2"),
    30000)
import time
time.sleep(1000000)

# print("pointquery el")
# client.pointQuery(
#     edgeListAlgorithm(),
#     fileOutputFormat.apply("/tmp/EdgeList"),
#     30000)


