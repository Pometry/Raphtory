package com.raphtory.test.allcommands

import com.raphtory.RaphtoryComponent
import scala.language.postfixOps

object AllCommandTest extends App {
  val partitionCount =10
  val routerCount =10
  new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  new RaphtoryComponent("watchdog",partitionCount,routerCount,1601)
  new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  new RaphtoryComponent("spout",partitionCount,routerCount,1603,"com.raphtory.test.allcommands.AllCommandsSpout")
  new RaphtoryComponent("router",partitionCount,routerCount,1604,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1605,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1606,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1607,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1608,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1609,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1610,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1611,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1612,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1613,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1614)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1617)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1618)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1619)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1620)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1621)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1622)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1623)

}

//Wanna check are all messages being ingested
//Is there a slow down over time (full range vs the final point in  time)
//Do changes make a difference to the run times
//How can we track the time taken by each stage of the analysis superstep


//0.72317415
//0.99089885
//0.2532931
//0.60880035
//0.80586946
//0.87541276
//0.7160485
//0.071917
//0.79626095
//0.57871693
//0.9081256


//Range Analysis Task received, your job ID is com.raphtory.algorithms.ConnectedComponents_1611587405024, running com.raphtory.algorithms.ConnectedComponents, between 1 and 300000 jumping 10000 at a time.
//{"time":1,"top5":[0],"total":0,"totalIslands":0,"proportion":0.0,"clustersGT2":0,"viewTime":222},
//{"time":10001,"top5":[54,53,37,29,29],"total":2703,"totalIslands":1146,"proportion":0.008081413,"clustersGT2":705,"viewTime":1439},
//{"time":20001,"top5":[6095,17,16,15,14],"total":1535,"totalIslands":955,"proportion":0.6945869,"clustersGT2":232,"viewTime":2880},
//{"time":30001,"top5":[8536,6,6,5,5],"total":731,"totalIslands":589,"proportion":0.9014679,"clustersGT2":39,"viewTime":3050},
//{"time":40001,"top5":[9219,5,4,3,3],"total":416,"totalIslands":382,"proportion":0.95276976,"clustersGT2":7,"viewTime":3942},
//{"time":50001,"top5":[9516,3,3,2,2],"total":245,"totalIslands":226,"proportion":0.9730061,"clustersGT2":3,"viewTime":4812},
//{"time":60001,"top5":[9623,2,2,2,2],"total":157,"totalIslands":149,"proportion":0.98334354,"clustersGT2":1,"viewTime":4961},
//{"time":70001,"top5":[9670,2,2,2,2],"total":115,"totalIslands":110,"proportion":0.9879444,"clustersGT2":1,"viewTime":5934},
//{"time":80001,"top5":[9715,3,2],"total":93,"totalIslands":90,"proportion":0.99031603,"clustersGT2":2,"viewTime":6385},
//{"time":90001,"top5":[9753],"total":74,"totalIslands":73,"proportion":0.99257076,"clustersGT2":1,"viewTime":6872},
//{"time":100001,"top5":[9761,2],"total":64,"totalIslands":62,"proportion":0.993486,"clustersGT2":1,"viewTime":7194},
//{"time":110001,"top5":[9741,2,2],"total":75,"totalIslands":72,"proportion":0.9922583,"clustersGT2":1,"viewTime":7423},
//{"time":120001,"top5":[9727,2,2,2],"total":82,"totalIslands":78,"proportion":0.9914382,"clustersGT2":1,"viewTime":7382},
//{"time":130001,"top5":[9764,3,2],"total":69,"totalIslands":66,"proportion":0.99278086,"clustersGT2":2,"viewTime":7886},
//{"time":140001,"top5":[9776,2],"total":66,"totalIslands":64,"proportion":0.99329406,"clustersGT2":1,"viewTime":7908},
//{"time":150001,"top5":[9783],"total":60,"totalIslands":59,"proportion":0.99400526,"clustersGT2":1,"viewTime":8912},
//{"time":160001,"top5":[9764],"total":65,"totalIslands":64,"proportion":0.993488,"clustersGT2":1,"viewTime":8494},
//{"time":170001,"top5":[9768,2],"total":65,"totalIslands":63,"proportion":0.9933896,"clustersGT2":1,"viewTime":8749},
//{"time":180001,"top5":[9752,2],"total":80,"totalIslands":78,"proportion":0.9918633,"clustersGT2":1,"viewTime":9468},
//{"time":190001,"top5":[9741,2,2,2],"total":90,"totalIslands":86,"proportion":0.99064374,"clustersGT2":1,"viewTime":9025},
//{"time":200001,"top5":[9779,2,2],"total":79,"totalIslands":76,"proportion":0.9918856,"clustersGT2":1,"viewTime":9347},
//{"time":210001,"top5":[9773],"total":63,"totalIslands":62,"proportion":0.993696,"clustersGT2":1,"viewTime":10031},
//{"time":220001,"top5":[9762,2,2],"total":73,"totalIslands":70,"proportion":0.99247664,"clustersGT2":1,"viewTime":10690},
//{"time":230001,"top5":[9754,2,2,2],"total":80,"totalIslands":76,"proportion":0.9916633,"clustersGT2":1,"viewTime":10353},
//{"time":240001,"top5":[9739,2,2],"total":85,"totalIslands":82,"proportion":0.9912468,"clustersGT2":1,"viewTime":10450},
//{"time":250001,"top5":[9770,2],"total":74,"totalIslands":72,"proportion":0.9924827,"clustersGT2":1,"viewTime":11057},
//{"time":260001,"top5":[9769,2,2],"total":63,"totalIslands":60,"proportion":0.9934913,"clustersGT2":1,"viewTime":11391},
//{"time":270001,"top5":[9757],"total":57,"totalIslands":56,"proportion":0.9942933,"clustersGT2":1,"viewTime":11427},
//{"time":280001,"top5":[9785],"total":57,"totalIslands":56,"proportion":0.99430954,"clustersGT2":1,"viewTime":11418},
//{"time":290001,"top5":[9755,2],"total":71,"totalIslands":69,"proportion":0.99277425,"clustersGT2":1,"viewTime":10887},
//{"time":300000,"top5":[9751,2,2,2],"total":74,"totalIslands":70,"proportion":0.9922662,"clustersGT2":1,"viewTime":11216},