from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.java_gateway import java_import
from py4j.java_collections import MapConverter
from raphtoryclient.serializers import read_int, UTF8Deserializer

rid = "raphtory_2132766705"

with open("/tmp/"+rid+"_python_gateway_connection_file", "rb") as info_file:
    gateway_port = read_int(info_file)
    gateway_secret = UTF8Deserializer().loads(info_file)

print("Setting up Java gateway...")
gateway = JavaGateway(
    gateway_parameters=GatewayParameters(port=gateway_port, auth_token=gateway_secret, auto_field=True))

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
customConfig = {"raphtory.deploy.id": rid, "raphtory.deploy.distributed": True}
mc_run_map_dict = MapConverter().convert(customConfig, gateway._gateway_client)
jmap = gateway.jvm.PythonUtils.toScalaMap(mc_run_map_dict)
client = raphtory.createClient(mc_run_map_dict)

print("Setting up the algorithm")
connectedComponentsAlgorithm = gateway.jvm.ConnectedComponents
fileOutputFormat = gateway.jvm.FileOutputFormat

print("Running a point query")
client.pointQuery(
    connectedComponentsAlgorithm(),
    fileOutputFormat.apply("/tmp/pythonCC2"),
    30000)



# edgeListAlgorithm = gateway.jvm.EdgeList

# long_class = gateway.jvm.Long
# long_array = gateway.new_array(long_class, 0)

# print("pointquery el")
# client.pointQuery(
#     edgeListAlgorithm(),
#     fileOutputFormat.apply("/tmp/EdgeList"),
#     30000)


