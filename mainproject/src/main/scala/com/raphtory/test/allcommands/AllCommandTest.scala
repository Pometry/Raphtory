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


//{"time":1,"windowsize":10000,"top5":[2],"total":1,"totalIslands":0,"proportion":1.0,"clustersGT2":0,"viewTime":2549},
//{"time":10001,"windowsize":10000,"top5":[5174,36,29,20,19],"total":1387,"totalIslands":646,"proportion":0.63082176,"clustersGT2":310,"viewTime":49271},
//{"time":20001,"windowsize":10000,"top5":[5238,15,15,14,12],"total":1385,"totalIslands":660,"proportion":0.64515334,"clustersGT2":317,"viewTime":10088},
//{"time":30001,"windowsize":10000,"top5":[5192,22,18,16,16],"total":1327,"totalIslands":605,"proportion":0.6362745,"clustersGT2":299,"viewTime":8383},
//{"time":40001,"windowsize":10000,"top5":[5164,22,21,19,17],"total":1374,"totalIslands":637,"proportion":0.63408643,"clustersGT2":306,"viewTime":8327},
//{"time":50001,"windowsize":10000,"top5":[5169,17,16,15,14],"total":1381,"totalIslands":639,"proportion":0.6331455,"clustersGT2":340,"viewTime":7489},
//{"time":60001,"windowsize":10000,"top5":[4987,31,24,24,22],"total":1417,"totalIslands":650,"proportion":0.60913646,"clustersGT2":333,"viewTime":8828},
//{"time":70001,"windowsize":10000,"top5":[5021,46,28,26,21],"total":1414,"totalIslands":632,"proportion":0.61411446,"clustersGT2":328,"viewTime":7627},
//{"time":80001,"windowsize":10000,"top5":[5159,26,18,18,16],"total":1358,"totalIslands":610,"proportion":0.63362813,"clustersGT2":320,"viewTime":11467},
//{"time":90001,"windowsize":10000,"top5":[5081,41,28,21,20],"total":1352,"totalIslands":609,"proportion":0.6231297,"clustersGT2":330,"viewTime":9575},
//{"time":100001,"windowsize":10000,"top5":[5127,28,28,25,19],"total":1406,"totalIslands":654,"proportion":0.6276934,"clustersGT2":335,"viewTime":9331},
//{"time":110001,"windowsize":10000,"top5":[5087,35,33,19,18],"total":1437,"totalIslands":691,"proportion":0.6197612,"clustersGT2":334,"viewTime":8534},
//{"time":120001,"windowsize":10000,"top5":[5162,49,24,23,20],"total":1350,"totalIslands":615,"proportion":0.63066584,"clustersGT2":306,"viewTime":7991},
//{"time":130001,"windowsize":10000,"top5":[5180,22,17,17,16],"total":1357,"totalIslands":609,"proportion":0.63573885,"clustersGT2":341,"viewTime":6847},
//{"time":140001,"windowsize":10000,"top5":[5171,35,17,16,15],"total":1369,"totalIslands":603,"proportion":0.6307636,"clustersGT2":325,"viewTime":9540},
//{"time":150001,"windowsize":10000,"top5":[5206,22,16,16,16],"total":1400,"totalIslands":647,"proportion":0.6345685,"clustersGT2":324,"viewTime":8550},
//{"time":160001,"windowsize":10000,"top5":[4959,45,29,22,21],"total":1454,"totalIslands":659,"proportion":0.6032113,"clustersGT2":332,"viewTime":7300},
//{"time":170001,"windowsize":10000,"top5":[5118,31,27,22,19],"total":1405,"totalIslands":670,"proportion":0.62551945,"clustersGT2":302,"viewTime":7347},
//{"time":180001,"windowsize":10000,"top5":[5270,19,17,14,13],"total":1369,"totalIslands":637,"proportion":0.6440968,"clustersGT2":319,"viewTime":8214},
//{"time":190001,"windowsize":10000,"top5":[4988,72,33,29,28],"total":1453,"totalIslands":672,"proportion":0.6050461,"clustersGT2":324,"viewTime":12156},
//{"time":200001,"windowsize":10000,"top5":[5125,32,27,22,20],"total":1378,"totalIslands":611,"proportion":0.6247714,"clustersGT2":314,"viewTime":11024},
//{"time":210001,"windowsize":10000,"top5":[4943,29,26,18,18],"total":1380,"totalIslands":596,"proportion":0.6039834,"clustersGT2":343,"viewTime":10205},
//{"time":220001,"windowsize":10000,"top5":[5196,44,41,24,23],"total":1372,"totalIslands":627,"proportion":0.63497496,"clustersGT2":320,"viewTime":8996},
//{"time":230001,"windowsize":10000,"top5":[4956,49,26,23,18],"total":1429,"totalIslands":665,"proportion":0.60358053,"clustersGT2":348,"viewTime":7892},
//{"time":240001,"windowsize":10000,"top5":[5171,34,30,21,19],"total":1337,"totalIslands":627,"proportion":0.63440067,"clustersGT2":310,"viewTime":9581},
//{"time":250001,"windowsize":10000,"top5":[5197,21,18,18,17],"total":1358,"totalIslands":625,"proportion":0.63517475,"clustersGT2":304,"viewTime":8277},
//{"time":260001,"windowsize":10000,"top5":[5014,39,34,32,19],"total":1405,"totalIslands":645,"proportion":0.6127337,"clustersGT2":320,"viewTime":9793},
//{"time":270001,"windowsize":10000,"top5":[5029,40,34,22,20],"total":1391,"totalIslands":631,"proportion":0.6145668,"clustersGT2":328,"viewTime":8790},
//{"time":280001,"windowsize":10000,"top5":[5126,38,28,21,21],"total":1394,"totalIslands":647,"proportion":0.6264206,"clustersGT2":341,"viewTime":8935},
//{"time":290001,"windowsize":10000,"top5":[5228,66,25,21,20],"total":1318,"totalIslands":611,"proportion":0.63771653,"clustersGT2":314,"viewTime":9553},
//{"time":300001,"windowsize":10000,"top5":[5149,24,24,21,18],"total":1383,"totalIslands":624,"proportion":0.6303869,"clustersGT2":305,"viewTime":8931},


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