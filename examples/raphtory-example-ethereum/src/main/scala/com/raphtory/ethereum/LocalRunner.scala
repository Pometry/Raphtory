package com.raphtory.ethereum

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.ethereum.graphbuilder.EthereumGraphBuilder
import com.raphtory.ethereum.analysis.Taint
import com.raphtory.sinks.FileSink
import com.raphtory.sinks.PulsarSink
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object LocalRunner extends App {

  // Import logger
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // Building the graph
  logger.info("Starting Ethereum application")

  // set up env
  val path = "/tmp/etherscan_tags.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/etherscan_tags.csv"
  FileUtils.curlFile(path, url)

  // create graph
  val source  = FileSpout("/tmp/etherscan_tags.csv")
  val builder = new EthereumGraphBuilder()
  val graph   = Raphtory.load(source, builder)

  // setup ethereum vars
  val startTime = 1574814233

  val infectedNodes: Set[String] =
    Set(
            "0xa09871aeadf4994ca12f5c0b6056bbd1d343c029"
    ) // 0xa09871aeadf4994ca12f5c0b6056bbd1d343c029") // upbit hacker 1 from  9007863 tx1  0xca4e0aa223e3190ab477efb25617eff3a42af7bdb29cdb7dc9e7935ea88626b4
  val stopNodes: Set[String] = Set(
          "0x001866ae5b3de6caa5a51543fd9fb64f524f5478",
          "0x00bdb5699745f5b860228c8f939abf1b9ae374ed",
          "0x00c5e04176d95a286fcce0e68c683ca0bfec8454",
          "0x0316eb71485b0ab14103307bf65a021042c6d380",
          "0x034f854b44d28e26386c1bc37ff9b20c6380b00d",
          "0x04645af26b54bd85dc02ac65054e87362a72cb22",
          "0x0536806df512d6cdde913cf95c9886f65b1d3462",
          "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
          "0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404",
          "0x059799f2261d37b829c2850cee67b5b975432271",
          "0x0681d8db095565fe8a346fa0277bffde9c0edbbf",
          "0x07ee55aa48bb72dcc6e9d78256648910de513eca",
          "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13",
          "0x0a98fb70939162725ae66e626fe4b52cff62c2e5",
          "0x0b95993a39a363d99280ac950f5e4536ab5c5566",
          "0x0c6c34cdd915845376fb5407e0895196c9dd4eec",
          "0x0c92efa186074ba716d0e2156a6ffabd579f8035",
          "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
          "0x0e55c54249f25f70d519b7fb1c20e3331e7ba76d",
          "0x1062a747393198f70f71ec65a582423dba7e5ab3",
          "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f",
          "0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5",
          "0x14d06788090769f669427b6aea1c0240d2321f34",
          "0x1522900b6dafac587d499a862861c0869be6e428",
          "0x170af0a02339743687afd3dc8d48cffd1f660728",
          "0x18916e1a2933cb349145a280473a5de8eb6630cb",
          "0x1a1918cb83ba146a1f4818bf34d92291ac9c0749",
          "0x1b93129f05cc2e840135aab154223c75097b69bf",
          "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c",
          "0x1c515eaa87568c850043a89c2d2c2e8187adb056",
          "0x1d1e10e8c66b67692f4c002c0cb334de5d485e41",
          "0x1ff8cdb51219a8838b52e9cac09b71e591bc998e",
          "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
          "0x229b5c097f9b35009ca1321ad2034d4b3d5070f6",
          "0x25c6459e5c5b01694f6453e8961420ccd1edf3b1",
          "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0",
          "0x28ffe35688ffffd0659aee2e34778b0ae4e193ad",
          "0x2903cadbe271e057edef157340b52a5898d7424f",
          "0x2910543af39aba0cd09dbb2d50200b3e800a63d2",
          "0x2984581ece53a4390d1f568673cf693139c97049",
          "0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3",
          "0x2e7542ec36df6429d8c397f88c4cf0c925948f44",
          "0x2ee3b2df6534abc759ffe994f7b8dcdfaa02cd31",
          "0x2f47a1c2db4a3b78cda44eade915c3b19107ddcc",
          "0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec",
          "0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1",
          "0x32598293906b5b17c27d657db3ad2c9b3f3e4265",
          "0x32be343b94f860124dc4fee278fdcbd38c102d88",
          "0x36928500bc1dcd7af6a2b4008875cc336b927d57",
          "0x36b01066b7fa4a0fdb2968ea0256c848e9135674",
          "0x3b46c790ff408e987928169bd1904b6d71c00305",
          "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be",
          "0x426903241ada3a0092c3493a0c795f2ec830d622",
          "0x46705dfff24256421a05d056c29e81bdc09723b8",
          "0x48ab9f29795dfb44b36587c50da4b30c0e84d3ed",
          "0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df",
          "0x4c766def136f59f6494f0969b1355882080cf8e0",
          "0x4d77a1144dc74f26838b69391a6d3b1e403d0990",
          "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67",
          "0x4fabb145d64652a948d72533023f6e7a623c7c53",
          "0x538d72ded42a76a30f730292da939e0577f22f57",
          "0x53d284357ec70ce289d6d64134dfac8e511c8a3d",
          "0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e",
          "0x564286362092d8e7936f0549571a803b203aaced",
          "0x5861b8446a2f6e19a067874c133f04c578928727",
          "0x5baeac0a0417a05733884852aa068b706967e790",
          "0x5c985e89dde482efe97ea9f1950ad149eb73829b",
          "0x5dbdebcae07cc958ba5290ff9deaae554e29e7b4",
          "0x60b45f993223dcb8bdf05e3391f7630e5a51d787",
          "0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea",
          "0x63155828e949aeba75038a5e8a5f4edd5038d9a6",
          "0x636b76ae213358b9867591299e5c62b8d014e372",
          "0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0",
          "0x66f820a414680b5bcda5eeca5dea238543f42054",
          "0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b",
          "0x6795cf8eb25585eadc356ae32ac6641016c550f2",
          "0x68065ca481bfb5e84c0d0e1ceae3021786f9aec6",
          "0x6874c552d3fd1879e3007d44ef54c8f940e84760",
          "0x69c6dcc8f83b196605fa1076897af0e7e2b6b044",
          "0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437",
          "0x6edb9d6547befc3397801c94bb8c97d2e8087e2f",
          "0x6ee0f7bb50a54ab5253da0667b0dc2ee526c30a8",
          "0x6f259637dcd74c767781e37bc6133cd6a68aa161",
          "0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380",
          "0x6f50c6bff08ec925232937b204b0ae23c488402a",
          "0x6f803466bcd17f44fa18975bf7c509ba64bf3825",
          "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8",
          "0x708396f17127c42383e3b9014072679b2f60b82f",
          "0x7180eb39a6264938fdb3effd7341c4727c382153",
          "0x73f8fc2e74302eb2efda125a326655acf0dc2d1b",
          "0x742d35cc6634c0532925a3b844bc454e4438f44e",
          "0x75a83599de596cbc91a1821ffa618c40e22ac8ca",
          "0x7727e5113d1d161373623e5f49fd568b4f543a9e",
          "0x7793cd85c11a924478d358d49b05b37e91b5810f",
          "0x7884f51dc1410387371ce61747cb6264e1daee0b",
          "0x794d28ac31bcb136294761a556b68d2634094153",
          "0x7ef35bb398e0416b81b019fea395219b65c52164",
          "0x85b931a32a0725be14285b66f1a22178c672d69b",
          "0x85c4edc43724e954e5849caaab61a26a9cb65f1b",
          "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa",
          "0x8aabba0077f1565df73e9d15dd3784a2b0033dad",
          "0x8b99f3660622e21f2910ecca7fbe51d654a1517d",
          "0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb",
          "0x8d5a41e85f4ce2433beef476305d307b9205d98d",
          "0x8e16bf47065fe843a82f4399baf5abac4e0822b7",
          "0x8e8bc99b79488c276d6f3ca11901e9abd77efea4",
          "0x8f22f2063d253846b53609231ed80fa571bc0c8f",
          "0x9007a0421145b06a0345d55a8c0f0327f62a2224",
          "0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325",
          "0x90f49e24a9554126f591d28174e157ca267194ba",
          "0x91dfa9d9e062a50d2f351bfba0d35a9604993dac",
          "0x926fc576b7facf6ae2d08ee2d4734c134a743988",
          "0x956e0dbecc0e873d34a5e39b25f364b2ca036730",
          "0x9a755332d874c893111207b0b220ce2615cd036f",
          "0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72",
          "0x9be89d2a4cd102d8fecc6bf9da793be995c22541",
          "0x9d6d492bd500da5b33cf95a5d610a73360fcaaa0",
          "0x9eebb2815dba2166d8287afa9a2c89336ba9deaa",
          "0xa23d7dd4b8a1060344caf18a29b42350852af481",
          "0xa2605e8679d399bb98fa8c2f0b4ef6d04f3e5c1a",
          "0xa3c1e324ca1ce40db73ed6026c4a177f099b5770",
          "0xa40dfee99e1c85dc97fdc594b16a460717838703",
          "0xa66daa57432024023db65477ba87d4e7f5f95213",
          "0xa8660c8ffd6d578f657b72c0c811284aef0b735e",
          "0xa910f92acdaf488fa6ef02174fb86208ad7722ba",
          "0xaa923cd02364bb8a4c3d6f894178d2e12231655c",
          "0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b",
          "0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5",
          "0xab5c66752a9e8167967685f1450532fb96d5d24f",
          "0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e",
          "0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4",
          "0xadb2b42f6bd96f5c65920b9ac88619dce4166f94",
          "0xb2a48f542dc56b89b24c04076cbe565b3dc58e7b",
          "0xb36efd48c9912bd9fd58b67b65f7438f6364a256",
          "0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9",
          "0xb794f5ea0ba39494ce839613fffba74279579268",
          "0xb8c77482e45f1f44de1745f52c74426c631bdd52",
          "0xb9a4873d8d2c22e56b8574e8605644d08e047549",
          "0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21",
          "0xbe0eb53f46cd790cd13851d5eff43d12404d33e8",
          "0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb",
          "0xc3501eaaf5e29aa0529342f409443120fd545ea7",
          "0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f",
          "0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828",
          "0xc837f51a0efa33f8eca03570e3d01a4b2cf97ffd",
          "0xc8b759860149542a98a3eb57c14aadf59d6d89b9",
          "0xc9a2c4868f0f96faaa739b59934dc9cb304112ec",
          "0xcac725bef4f114f728cbcfd744a731c2a463c3fc",
          "0xcafb10ee663f465f9d10588ac44ed20ed608c11e",
          "0xcbd658da220e4ac1fc4367f30c3c107dbb3b52a1",
          "0xd0b0d5a8c0b40b7272115a23a2d5e36ad190f13c",
          "0xd10e08325c0e95d59c607a693483680fe5b755b3",
          "0xd24400ae8bfebb18ca49be86258a3c749cf46853",
          "0xd3a2f775e973c1671f2047e620448b8662dcd3ca",
          "0xd551234ae421e3bcba99a0da6d736074f22192ff",
          "0xd793281182a0e3e023116004778f45c29fc14f19",
          "0xd8a83b72377476d0a66683cde20a8aad0b628713",
          "0xda1e5d4cc9873963f788562354b55a772253b92f",
          "0xdc76cd25977e0a5ae17155770273ad58648900d3",
          "0xdd51f01d9fc0fd084c1a4737bbfa5becb6ced9bc",
          "0xdec042a90de005b22754e94a8a979c4b8c67fde5",
          "0xdf95de30cdff4381b69f9e4fa8dddce31a0128df",
          "0xe0f0cfde7ee664943906f17f7f14342e76a5cec7",
          "0xe1f3c653248de6894d683cb2f10de7ca2253046f",
          "0xe2bf37054c40fb6113b7b17e7dcd340e67d0aba6",
          "0xe3314bbf3334228b257779e28228cfb86fa4261b",
          "0xe853c56864a2ebe4576a807d26fdc4a0ada51919",
          "0xe8ed915e208b28c617d20f3f8ca8e11455933adf",
          "0xe9319eba87af7c2fc1f55ccde9d10ea8efbd592d",
          "0xe93381fb4c4f14bda253907b18fad305d799241a",
          "0xe94b04a0fed112f3664e45adb2b8915693dd5ff3",
          "0xea0cfef143182d7b9208fbfeda9d172c2aced972",
          "0xead6be34ce315940264519f250d8160f369fa5cd",
          "0xeb6d43fe241fb2320b5a3c9be9cdfd4dd8226451",
          "0xecd8b3877d8e7cd0739de18a5b545bc0b3538566",
          "0xeec606a66edb6f497662ea31b5eb1610da87ab5f",
          "0xeee28d484628d41a82d01e21d12e2e78d69920da",
          "0xef54f559b5e3b55b783c7bc59850f83514b6149c",
          "0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94",
          "0xf66852bc122fd40bfecc63cd48217e88bda12109",
          "0xf775a9a0ad44807bc15936df0ee68902af1a0eee",
          "0xf7a8af16acb302351d7ea26ffc380575b741724c",
          "0xf977814e90da44bfa03b6295a0616a897441acec",
          "0xfa4b5be3f2f84f56703c42eb22142744e95a2c58",
          "0xfa52274dd61e1643d2205169732f29114bc240b3",
          "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98",
          "0xfbf2173154f7625713be22e0504404ebfe021eae",
          "0xfca70e67b3f93f679992cd36323eeb5a5370c8e4",
          "0xfd54078badd5653571726c3370afb127351a6f26",
          "0xfdb16996831753d5331ff813c29a93c76834a0ad",
          "0xfe9e8709d3215310075d67e3ed32a380ccf451c8",
          "0xff0a024b66739357c4ed231fb3dbc0c8c22749f5"
  )

  // execute queries
  // val graphQuery = new TaintAlgorithm(startTime, infectedNodes, stopNodes)
  val fileOutput   = FileSink("/tmp/ethereum/Taint")
  val pulsarOutput = PulsarSink("TaintTracking")
  graph
    .at(1575013446)
    .past()
    .execute(Taint(startTime, infectedNodes, stopNodes))
    .writeTo(pulsarOutput)
  // graph.pointQuery(ConnectedComponents(), FileOutputFormat("/tmp/ethereum/connected_components"), 1591951621)

  // Range query, run the same command over certain time periods
  // analysis a day at a home, a day/week/month
  // this can be sent over any time paramaters
  //graph.rangeQuery(
  //    ConnectedComponents("/tmp/connected_components"),
  //    1574814233,
  //    1575013457,
  //    86400,
  //    List(2592000, 604800,86400)
  //)

  // run the query every minute
  // graph.liveQuery(ConnectedComponents("/tmp/connected_components"), 60)
}
