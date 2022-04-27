from raphtoryclient import client

raphtoryClient = client.client(raphtory_deployment_id="raphtory_2135049896")

raphtoryClient.java_import("com.raphtory.algorithms.generic.ConnectedComponents")
connectedComponentsAlgorithm = raphtoryClient.gateway.jvm.ConnectedComponents

raphtoryClient.java_import("com.raphtory.output.FileOutputFormat")
fileOutputFormat = raphtoryClient.gateway.jvm.FileOutputFormat

raphtoryClient.raphtory_client.pointQuery(
    connectedComponentsAlgorithm(),
    fileOutputFormat.apply("/tmp/adsasdads"),
    30000)


from py4j.java_collections import MapConverter
customConfig = {"raphtory.deploy.id": "raphtory_2135049896"}
mc_run_map_dict = MapConverter().convert(customConfig, raphtoryClient.gateway)
jmap = raphtoryClient.gateway.jvm.PythonUtils.toScalaMap(mc_run_map_dict)
deployedGraph = raphtoryClient.raphtory_client.deployedGraph(jmap)



# queryHandler = raphtoryClient.raphtory_client
#     .at(30000)
#     .past()
#     .execute(connectedComponentsAlgorithm())
#     .writeTo(fileOutputFormat.apply("/tmp/pythonCC"))



# raphtoryClient.raphtory_client.pointQuery(
#     connectedComponentsAlgorithm(),
#     ,
#     30000)
