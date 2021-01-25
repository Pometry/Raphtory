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



//{"time":1,"windowsize":10000,"top5":[0],"total":0,"totalIslands":0,"proportion":0.0,"clustersGT2":0,"viewTime":3400},
//{"time":10001,"windowsize":10000,"top5":[56,46,43,41,38],"total":2763,"totalIslands":1230,"proportion":0.008407146,"clustersGT2":703,"viewTime":11062},
//{"time":20001,"windowsize":10000,"top5":[74,55,47,44,44],"total":2685,"totalIslands":1215,"proportion":0.011053025,"clustersGT2":690,"viewTime":3446},
//{"time":30001,"windowsize":10000,"top5":[57,51,50,42,38],"total":2651,"totalIslands":1167,"proportion":0.008588217,"clustersGT2":705,"viewTime":2957},
//{"time":40001,"windowsize":10000,"top5":[84,39,32,31,25],"total":2705,"totalIslands":1188,"proportion":0.01287159,"clustersGT2":721,"viewTime":3029},
//{"time":50001,"windowsize":10000,"top5":[42,42,36,29,29],"total":2711,"totalIslands":1168,"proportion":0.006365565,"clustersGT2":741,"viewTime":2337},
//{"time":60001,"windowsize":10000,"top5":[51,37,37,36,34],"total":2629,"totalIslands":1181,"proportion":0.0077039273,"clustersGT2":669,"viewTime":2331},
//{"time":70001,"windowsize":10000,"top5":[132,64,33,32,29],"total":2727,"totalIslands":1212,"proportion":0.01960202,"clustersGT2":702,"viewTime":3865},
//{"time":80001,"windowsize":10000,"top5":[85,37,34,31,31],"total":2722,"totalIslands":1195,"proportion":0.0127417175,"clustersGT2":695,"viewTime":2827},
//{"time":90001,"windowsize":10000,"top5":[69,37,36,36,30],"total":2703,"totalIslands":1171,"proportion":0.010394697,"clustersGT2":703,"viewTime":2359},
//{"time":100001,"windowsize":10000,"top5":[78,71,40,30,28],"total":2625,"totalIslands":1217,"proportion":0.011902945,"clustersGT2":684,"viewTime":2844},
//{"time":110001,"windowsize":10000,"top5":[56,54,43,38,37],"total":2637,"totalIslands":1165,"proportion":0.0085444,"clustersGT2":713,"viewTime":3007},
//{"time":120001,"windowsize":10000,"top5":[46,42,40,34,31],"total":2681,"totalIslands":1164,"proportion":0.0069100196,"clustersGT2":723,"viewTime":2113},
//{"time":130001,"windowsize":10000,"top5":[58,39,34,32,31],"total":2712,"totalIslands":1220,"proportion":0.008685235,"clustersGT2":682,"viewTime":2759},
//{"time":140001,"windowsize":10000,"top5":[92,40,30,27,26],"total":2769,"totalIslands":1242,"proportion":0.013764213,"clustersGT2":689,"viewTime":2322},
//{"time":150001,"windowsize":10000,"top5":[69,45,43,43,38],"total":2724,"totalIslands":1181,"proportion":0.010419813,"clustersGT2":725,"viewTime":2317},
//{"time":160001,"windowsize":10000,"top5":[67,51,44,38,29],"total":2724,"totalIslands":1180,"proportion":0.010102533,"clustersGT2":712,"viewTime":2804},
//{"time":170001,"windowsize":10000,"top5":[69,45,41,37,37],"total":2672,"totalIslands":1211,"proportion":0.01047836,"clustersGT2":712,"viewTime":2461},
//{"time":180001,"windowsize":10000,"top5":[39,36,33,29,28],"total":2712,"totalIslands":1175,"proportion":0.005899259,"clustersGT2":712,"viewTime":2142},
//{"time":190001,"windowsize":10000,"top5":[34,33,31,31,31],"total":2695,"totalIslands":1177,"proportion":0.0051344004,"clustersGT2":714,"viewTime":2204},
//{"time":200001,"windowsize":10000,"top5":[74,31,23,22,22],"total":2677,"totalIslands":1184,"proportion":0.011151296,"clustersGT2":743,"viewTime":2591},
//{"time":210001,"windowsize":10000,"top5":[57,50,38,32,29],"total":2712,"totalIslands":1180,"proportion":0.008494784,"clustersGT2":708,"viewTime":2719},
//{"time":220001,"windowsize":10000,"top5":[97,59,36,30,26],"total":2693,"totalIslands":1225,"proportion":0.0147237405,"clustersGT2":691,"viewTime":3877},
//{"time":230001,"windowsize":10000,"top5":[73,48,32,28,26],"total":2646,"totalIslands":1130,"proportion":0.011035525,"clustersGT2":694,"viewTime":2615},
//{"time":240001,"windowsize":10000,"top5":[45,43,34,33,27],"total":2683,"totalIslands":1204,"proportion":0.006860802,"clustersGT2":683,"viewTime":1863},
//{"time":250001,"windowsize":10000,"top5":[69,52,47,38,37],"total":2779,"totalIslands":1235,"proportion":0.010413523,"clustersGT2":729,"viewTime":2964},
//{"time":260001,"windowsize":10000,"top5":[42,34,29,28,28],"total":2681,"totalIslands":1158,"proportion":0.0063492064,"clustersGT2":692,"viewTime":2284},
//{"time":270001,"windowsize":10000,"top5":[55,44,42,36,26],"total":2738,"totalIslands":1242,"proportion":0.008403362,"clustersGT2":708,"viewTime":2158},
//{"time":280001,"windowsize":10000,"top5":[82,57,57,33,31],"total":2667,"totalIslands":1152,"proportion":0.01247338,"clustersGT2":678,"viewTime":3453},
//{"time":290001,"windowsize":10000,"top5":[76,54,48,44,40],"total":2682,"totalIslands":1222,"proportion":0.011679729,"clustersGT2":672,"viewTime":2841},
//{"time":300000,"windowsize":10000,"top5":[45,37,36,36,34],"total":2693,"totalIslands":1209,"proportion":0.0067658997,"clustersGT2":696,"viewTime":2451},

//{"time":1,"windowsize":10000,"top5":[0],"total":0,"totalIslands":0,"proportion":0.0,"clustersGT2":0,"viewTime":2602},
//{"time":10001,"windowsize":10000,"top5":[56,46,43,41,38],"total":2763,"totalIslands":1230,"proportion":0.008407146,"clustersGT2":703,"viewTime":28280},
//{"time":20001,"windowsize":10000,"top5":[74,55,47,44,44],"total":2685,"totalIslands":1215,"proportion":0.011053025,"clustersGT2":690,"viewTime":3630},
//{"time":30001,"windowsize":10000,"top5":[57,51,50,42,38],"total":2651,"totalIslands":1167,"proportion":0.008588217,"clustersGT2":705,"viewTime":3022},
//{"time":40001,"windowsize":10000,"top5":[84,39,32,31,25],"total":2705,"totalIslands":1188,"proportion":0.01287159,"clustersGT2":721,"viewTime":3157},
//{"time":50001,"windowsize":10000,"top5":[42,42,36,29,29],"total":2711,"totalIslands":1168,"proportion":0.006365565,"clustersGT2":741,"viewTime":2193},
//{"time":60001,"windowsize":10000,"top5":[51,37,37,36,34],"total":2629,"totalIslands":1181,"proportion":0.0077039273,"clustersGT2":669,"viewTime":2405},
//{"time":70001,"windowsize":10000,"top5":[132,64,33,32,29],"total":2727,"totalIslands":1212,"proportion":0.01960202,"clustersGT2":702,"viewTime":3686},
//{"time":80001,"windowsize":10000,"top5":[85,37,34,31,31],"total":2722,"totalIslands":1195,"proportion":0.0127417175,"clustersGT2":695,"viewTime":3026},
//{"time":90001,"windowsize":10000,"top5":[69,37,36,36,30],"total":2703,"totalIslands":1171,"proportion":0.010394697,"clustersGT2":703,"viewTime":2462},
//{"time":100001,"windowsize":10000,"top5":[78,71,40,30,28],"total":2625,"totalIslands":1217,"proportion":0.011902945,"clustersGT2":684,"viewTime":2775},
//{"time":110001,"windowsize":10000,"top5":[56,54,43,38,37],"total":2637,"totalIslands":1165,"proportion":0.0085444,"clustersGT2":713,"viewTime":3132},
//{"time":120001,"windowsize":10000,"top5":[46,42,40,34,31],"total":2681,"totalIslands":1164,"proportion":0.0069100196,"clustersGT2":723,"viewTime":2066},
//{"time":130001,"windowsize":10000,"top5":[58,39,34,32,31],"total":2712,"totalIslands":1220,"proportion":0.008685235,"clustersGT2":682,"viewTime":3312},
//{"time":140001,"windowsize":10000,"top5":[92,40,30,27,26],"total":2769,"totalIslands":1242,"proportion":0.013764213,"clustersGT2":689,"viewTime":3190},
//{"time":150001,"windowsize":10000,"top5":[69,45,43,43,38],"total":2724,"totalIslands":1181,"proportion":0.010419813,"clustersGT2":725,"viewTime":2845},
//{"time":160001,"windowsize":10000,"top5":[67,51,44,38,29],"total":2724,"totalIslands":1180,"proportion":0.010102533,"clustersGT2":712,"viewTime":3738},
//{"time":170001,"windowsize":10000,"top5":[69,45,41,37,37],"total":2672,"totalIslands":1211,"proportion":0.01047836,"clustersGT2":712,"viewTime":3290},
//{"time":180001,"windowsize":10000,"top5":[39,36,33,29,28],"total":2712,"totalIslands":1175,"proportion":0.005899259,"clustersGT2":712,"viewTime":2751},
//{"time":190001,"windowsize":10000,"top5":[34,33,31,31,31],"total":2695,"totalIslands":1177,"proportion":0.0051344004,"clustersGT2":714,"viewTime":2564},
//{"time":200001,"windowsize":10000,"top5":[74,31,23,22,22],"total":2677,"totalIslands":1184,"proportion":0.011151296,"clustersGT2":743,"viewTime":2901},
//{"time":210001,"windowsize":10000,"top5":[57,50,38,32,29],"total":2712,"totalIslands":1180,"proportion":0.008494784,"clustersGT2":708,"viewTime":3193},
//{"time":220001,"windowsize":10000,"top5":[97,59,36,30,26],"total":2693,"totalIslands":1225,"proportion":0.0147237405,"clustersGT2":691,"viewTime":4365},
//{"time":230001,"windowsize":10000,"top5":[73,48,32,28,26],"total":2646,"totalIslands":1130,"proportion":0.011035525,"clustersGT2":694,"viewTime":2795},
//{"time":240001,"windowsize":10000,"top5":[45,43,34,33,27],"total":2683,"totalIslands":1204,"proportion":0.006860802,"clustersGT2":683,"viewTime":3107},
//{"time":250001,"windowsize":10000,"top5":[69,52,47,38,37],"total":2779,"totalIslands":1235,"proportion":0.010413523,"clustersGT2":729,"viewTime":2594},
//{"time":260001,"windowsize":10000,"top5":[42,34,29,28,28],"total":2681,"totalIslands":1158,"proportion":0.0063492064,"clustersGT2":692,"viewTime":2545},
//{"time":270001,"windowsize":10000,"top5":[55,44,42,36,26],"total":2738,"totalIslands":1242,"proportion":0.008403362,"clustersGT2":708,"viewTime":2076},
//{"time":280001,"windowsize":10000,"top5":[82,57,57,33,31],"total":2667,"totalIslands":1152,"proportion":0.01247338,"clustersGT2":678,"viewTime":3665},
//{"time":290001,"windowsize":10000,"top5":[76,54,48,44,40],"total":2682,"totalIslands":1222,"proportion":0.011679729,"clustersGT2":672,"viewTime":2910},
//{"time":300000,"windowsize":10000,"top5":[45,37,36,36,34],"total":2693,"totalIslands":1209,"proportion":0.0067658997,"clustersGT2":696,"viewTime":2596},
