from py4j.java_gateway import java_import, JavaGateway


# TODO START A JVM, PY4J DOES NOT DO THAT FOR YOU
# TODO BASH RUNNER?
gateway = JavaGateway()
java_import(gateway.jvm, "com.raphtory.deployment.Raphtory")
java_import(gateway.jvm, "com.raphtory.algorithms.generic.ConnectedComponents")
java_import(gateway.jvm, "com.raphtory.output.FileOutputFormat")
Raphtory = gateway.jvm.com.raphtory.deployment.Raphtory
graph = Raphtory.createClient()
cc = gateway.jvm.com.raphtory.algorithms.generic.ConnectedComponents
fof = gateway.jvm.com.raphtory.output.FileOutputFormat
graph.pointQuery(cc(), fof("/tmp"), 1591951621)


