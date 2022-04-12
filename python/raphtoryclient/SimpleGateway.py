from py4j.java_gateway import JavaGateway, GatewayParameters, get_java_class
from py4j.java_gateway import java_import

gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True))

java_import(gateway.jvm, "com.raphtory.deployment.Raphtory")
java_import(gateway.jvm, "com.raphtory.spouts.FileSpout")
java_import(gateway.jvm, "com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder")
java_import(gateway.jvm, "scala.immutable")
java_import(gateway.jvm, "scala.collection.JavaConverters")
java_import(gateway.jvm, "com.raphtory.examples.lotrTopic.PythonUtils")
java_import(gateway.jvm,'java.util.*')
# java_import(gateway.jvm, "scala.reflect.ClassTag")
# java_import(gateway.jvm, "scala.reflect.runtime.universe._")
# java_import(gateway.jvm, "scala.reflect.api.TypeTags")

source = gateway.jvm.FileSpout.apply("/Users/haaroony/Documents/Raphtory/examples/raphtory-example-lotr/lotr.csv")
java_map = gateway.jvm.java.util.Map.of("", "")
scala_map = gateway.jvm.PythonUtils.toScalaMap(java_map)

builder = gateway.jvm.LOTRGraphBuilder()
raphtory = gateway.jvm.Raphtory

jarr = gateway.new_array(gateway.jvm.java.lang.String, len([]))
ss = gateway.jvm.java.lang.String
string_class = get_java_class(ss)


graph = raphtory.streamGraph(source, builder, scala_map, string_class, ss)

# export PYTHONPATH=/Users/haaroony/Downloads/py4j-0.10.9.3.zip:/Users/haaroony/.virtualenvs/pyConnectedComps/bin/python