package com.raphtory.examples.blockchain.analysers

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.util.Random

class TaintTrackExchangeStop(args:Array[String]) extends Analyser(args) {
  //val infectedNode =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_BAD_ACTOR", "0xa09871aeadf4994ca12f5c0b6056bbd1d343c029").trim
  //val infectionStartingBlock =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_START_BLOCK", "9007863").trim.toLong
  //val infectedNode =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_BAD_ACTOR", "0x52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5").trim.toLowerCase.asInstanceOf[String]
  //val infectionStartingBlock =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_START_BLOCK", "4000000").trim.toLong
  val infectedNode = args(0).trim.toLowerCase
  val infectionStartingBlock = args(1).trim.toLong
  val listOfExchanges = Array("0x83053C32b7819F420dcFed2D218335fe430Fe3b5,0x05f51aab068caa6ab7eeb672f88c180f67f17ec7","0x2ddd202174a72514ed522e77972b461b03155525","0xf0c80fb9fb22bef8269cb6feb9a51130288a671f","0x94597850916a49b3b152ee374e97260b99249f5b","0x4df5f3610e2471095a130d7d934d551f3dde01ed","0xadb72986ead16bdbc99208086bd431c1aa38938e","0x7a10ec7d68a048bdae36a70e93532d31423170fa","0xce1bf8e51f8b39e51c6184e059786d1c0eaf360f","0xf73c3c65bde10bf26c2e1763104e609a41702efe","0x0bb5de248dbbd31ee6c402c3c4a70293024acf74","0xed5cdb0d02152046e6f234ad578613831b9184d4","0xa30d8157911ef23c46c0eb71889efe6a648a41f7","0x6eff3372fa352b239bb24ff91b423a572347000d","0xf7793d27a1b76cdf14db7c83e82c772cf7c92910","0xcce8d59affdd93be338fc77fa0a298c2cb65da59","0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be","0xd551234ae421e3bcba99a0da6d736074f22192ff","0x564286362092d8e7936f0549571a803b203aaced","0x0681d8db095565fe8a346fa0277bffde9c0edbbf","0xfe9e8709d3215310075d67e3ed32a380ccf451c8","0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67","0xbe0eb53f46cd790cd13851d5eff43d12404d33e8","0xf977814e90da44bfa03b6295a0616a897441acec","0x001866ae5b3de6caa5a51543fd9fb64f524f5478","0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4","0x4b729cf402cfcffd057e254924b32241aedc1795","0x7c49e1c0e33f3efb57d64b7690fa287c8d15b90a","0x5d375281582791a38e0348915fa9cbc6139e9c2a","0xdf5021a4c1401f1125cd347e394d977630e17cf7","0x28ebe764b8f9a853509840645216d3c2c0fd774b","0x1151314c646ce4e0efd76d1af4760ae66a9fe30f","0x742d35cc6634c0532925a3b844bc454e4438f44e","0x876eabf441b2ee5b5b0554fd502a8e0600950cfa","0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e","0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828","0x88d34944cf554e9cccf4a24292d891f620e9c94f","0x186549a4ae594fc1f70ba4cffdac714b405be3f9","0xd273bd546b11bd60214a2f9d71f22a088aafe31b","0x3052cd6bf951449a984fe4b5a38b46aef9455c8e","0x2140efd7ba31169c69dfff6cdc66c542f0211825","0xa0ff1e0f30b5dda2dc01e7e828290bc72b71e57d","0xc1da8f69e4881efe341600620268934ef01a3e63","0xb4460b75254ce0563bb68ec219208344c7ea838c","0x15878e87c685f866edfaf454be6dc06fa517b35b","0x31d03f07178bcd74f9099afebd23b0ae30184ab5","0xed48dc0628789c2956b1e41726d062a86ec45bff","0x3fbe1f8fc5ddb27d428aa60f661eaaab0d2000ce","0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1","0x03bdf69b1322d623836afbd27679a1c0afa067e9","0x4b1a99467a284cc690e3237bc69105956816f762","0x986a2fca9eda0e06fbf7839b89bfc006ee2a23dd","0x00bdb5699745f5b860228c8f939abf1b9ae374ed","0x1522900b6dafac587d499a862861c0869be6e428","0x9a9bed3eb03e386d66f8a29dc67dc29bbb1ccb72","0x059799f2261d37b829c2850cee67b5b975432271","0x4c766def136f59f6494f0969b1355882080cf8e0","0xc5b611f502a0dcf6c3188fd494061ae29b2baa4f","0x1b8a38ea02ceda9440e00c1aeba26ee2dc570423","0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98","0xe94b04a0fed112f3664e45adb2b8915693dd5ff3","0x66f820a414680b5bcda5eeca5dea238543f42054","0xaa90b4aae74cee41e004bc45e45a427406c4dcae","0xf8d04a720520d0bcbc722b1d21ca194aa22699f2","0xfb9f7f41319157ac5c5dccae308a63a4337ad5d9","0x00cdc153aa8894d08207719fe921fff964f28ba3","0x007174732705604bbbf77038332dc52fd5a5000c","0x1c00d840ccaa67c494109f46e55cfeb2d8562f5c","0x73957709695e73fd175582105c44743cf0fb6f2f","0xd7c866d0d536937bf9123e02f7c052446588189f","0x4dc98c79a52968a6c20ce9a7a08d5e8d1c2d5605","0x88988d6ef12d7084e34814b9edafa01ae0d05082","0x72bcfa6932feacd91cb2ea44b0731ed8ae04d0d3","0x7a56f645dcb513d0326cbaa048e9106ff6d4cd5f","0xfd648cc72f1b4e71cbdda7a0a91fe34d32abd656","0x96fc4553a00c117c5b0bed950dd625d1c16dc894","0x8958618332df62af93053cb9c535e26462c959b0","0xb726da4fbdc3e4dbda97bb20998cf899b0e727e0","0x9539e0b14021a43cde41d9d45dc34969be9c7cb0","0x33683b94334eebc9bd3ea85ddbda4a86fb461405","0xb6ba1931e4e74fd080587688f6db10e830f810d5","0xb9ee1e551f538a464e8f8c41e9904498505b49b0","0x4b01721f0244e7c5b5f63c20942850e447f5a5ee","0xd4bddf5e3d0435d7a6214a0b949c7bb58621f37c","0xf2d4766ad705e3a5c9ba5b0436b473085f82f82f","0xa270f3ad1a7a82e6a3157f12a900f1e25bc4fbfd","0x167a9333bf582556f35bd4d16a7e80e191aa6476","0x1e2fcfd26d36183f1a5d90f0e6296915b02bcb40","0xd0808da05cc71a9f308d330bc9c5c81bbc26fc59","0x0d6b5a54f940bf3d52e438cab785981aaefdf40c","0xd1560b3984b7481cd9a8f40435a53c860187174d","0x521db06bf657ed1d6c98553a70319a8ddbac75a3","0x5baeac0a0417a05733884852aa068b706967e790","0x2984581ece53a4390d1f568673cf693139c97049","0xe17ee7b3c676701c66b395a35f0df4c2276a344e","0xf1c525a488a848b58b95d79da48c21ce434290f7","0x8d76166c22658a144c0211d87abf152e6a2d9d95","0xd3808c5d48903be1490989f3fce2a2b3890e8eb6","0x1fd6267f0d86f62d88172b998390afee2a1f54b6","0xd7b9a9b2f665849c4071ad5af77d8c76aa30fb32","0x94fe3ad91dacba8ec4b82f56ff7c122181f1535d","0x915d7915f2b469bb654a7d903a5d4417cb8ea7df","0x4e5b2e1dc63f6b91cb6cd759936495434c7e972f","0x0021845f4c2604c58f9ba5b7bff58d16a2ab372c","0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2","0x0d0707963952f2fba59dd06f2b425ace40b492fe","0x7793cd85c11a924478d358d49b05b37e91b5810f","0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c","0xd793281182a0e3e023116004778f45c29fc14f19","0x9f5ca0012b9b72e8f3db57092a6f26bf4f13dc69","0xd24400ae8bfebb18ca49be86258a3c749cf46853","0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8","0x61edcdf5bb737adffe5043706e7c5bb1f1a56eea","0x07ee55aa48bb72dcc6e9d78256648910de513eca","0x9fb01a2584aac5aae3fab1ed25f86c5269b32999","0x9c67e141c0472115aa1b98bd0088418be68fd249","0x59a5208b32e627891c389ebafc644145224006e8","0xa12431d0b9db640034b0cdfceef9cce161e62be4","0x274f3c32c90517975e29dfc209a23f315c1e5fc7","0x8533a0bd9310eb63e7cc8e1116c18a3d67b1976a","0xab5c66752a9e8167967685f1450532fb96d5d24f","0xe93381fb4c4f14bda253907b18fad305d799241a","0xfa4b5be3f2f84f56703c42eb22142744e95a2c58","0x46705dfff24256421a05d056c29e81bdc09723b8","0x32598293906b5b17c27d657db3ad2c9b3f3e4265","0x5861b8446a2f6e19a067874c133f04c578928727","0x926fc576b7facf6ae2d08ee2d4734c134a743988","0xeec606a66edb6f497662ea31b5eb1610da87ab5f","0x7ef35bb398e0416b81b019fea395219b65c52164","0x229b5c097f9b35009ca1321ad2034d4b3d5070f6","0xd8a83b72377476d0a66683cde20a8aad0b628713","0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b","0x90e9ddd9d8d5ae4e3763d0cf856c97594dea7325","0x18916e1a2933cb349145a280473a5de8eb6630cb","0x6f48a3e70f0251d1e83a989e62aaa2281a6d5380","0xf056f435ba0cc4fcd2f1b17e3766549ffc404b94","0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5","0x5401dbf7da53e1c9dbf484e3d69505815f2f5e6e","0x034f854b44d28e26386c1bc37ff9b20c6380b00d","0x0577a79cfc63bbc0df38833ff4c4a3bf2095b404","0x0c6c34cdd915845376fb5407e0895196c9dd4eec","0x794d28ac31bcb136294761a556b68d2634094153","0xfdb16996831753d5331ff813c29a93c76834a0ad","0xfd54078badd5653571726c3370afb127351a6f26","0xb4cd0386d2db86f30c1a11c2b8c4f4185c1dade9","0x4d77a1144dc74f26838b69391a6d3b1e403d0990","0x28ffe35688ffffd0659aee2e34778b0ae4e193ad","0xcac725bef4f114f728cbcfd744a731c2a463c3fc","0xeee28d484628d41a82d01e21d12e2e78d69920da","0x5c985e89dde482efe97ea9f1950ad149eb73829b","0xdc76cd25977e0a5ae17155770273ad58648900d3","0xadb2b42f6bd96f5c65920b9ac88619dce4166f94","0xa8660c8ffd6d578f657b72c0c811284aef0b735e","0x1062a747393198f70f71ec65a582423dba7e5ab3","0x3c11c3025ce387d76c2eddf1493ec55a8cc2a0f7","0x51836a753e344257b361519e948ffcaf5fb8d521","0x9cbadd5ce7e14742f70414a6dcbd4e7bb8712719","0xb1a34309af7f29b4195a6b589737f86e14597ddc","0x2910543af39aba0cd09dbb2d50200b3e800a63d2","0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13","0xe853c56864a2ebe4576a807d26fdc4a0ada51919","0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0","0xfa52274dd61e1643d2205169732f29114bc240b3","0xe8a0e282e6a3e8023465accd47fae39dd5db010b","0x629a7144235259336ea2694167f3c8b856edd7dc","0x30b71d015f60e2f959743038ce0aaec9b4c1ea44","0x2b5634c42055806a59e9107ed44d43c426e58258","0x689c56aef474df92d44a1b70850f808488f9769c","0xea81ce54a0afa10a027f65503bd52fba83d745b8","0x0861fca546225fbf8806986d211c8398f7457734","0x7891b20c690605f4e370d6944c8a5dbfac5a451c","0x1b6c1a0e20af81b922cb454c3e52408496ee7201","0x8271b2e8cbe29396e9563229030c89679b9470db","0x5e575279bf9f4acf0a130c186861454247394c06","0xedbb72e6b3cf66a792bff7faac5ea769fe810517","0x243bec9256c9a3469da22103891465b47583d9f1","0xe03c23519e18d64f144d2800e30e81b0065c48b5","0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88","0x0211f3cedbef3143223d3acf0e589747933e8527","0xae7006588d03bd15d6954e3084a7e644596bc251","0x6cc5f688a315f3dc28a7781717a9a798a59fda7b","0x236f9f97e0e62388479bf9e5ba4889e46b0273c3","0xa7efae728d2936e78bda97dc267687568dd593f3","0x03e3ff995863828554282e80870b489cc31dc8bc","0xaeec6f5aca72f3a005af1b3420ab8c8c7009bac8","0xbd8ef191caa1571e8ad4619ae894e07a75de0c35","0x2bb97b6cf6ffe53576032c11711d59bd056830ee","0xfb90501083a3b6af766c8da35d3dde01eb0d2a68","0xabc74170f3cb8ab352820c39cc1d1e05ce9e41d3","0xd4dcd2459bb78d7a645aa7e196857d421b10d93f","0x32be343b94f860124dc4fee278fdcbd38c102d88","0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef","0xb794f5ea0ba39494ce839613fffba74279579268","0xa910f92acdaf488fa6ef02174fb86208ad7722ba","0xaa9fa73dfe17ecaa2c89b39f0bb2779613c5fc3b","0x2fa2bc2ce6a4f92952921a4caa46b3727d24a1ec","0x31a2feb9b5d3b5f4e76c71d6c92fc46ebb3cb1c1","0x6b71834d65c5c4d8ed158d54b47e6ea4ff4e5437","0x48d466b7c0d32b61e8a82cd2bcf060f7c3f966df","0x0536806df512d6cdde913cf95c9886f65b1d3462","0x8d451ae5ee8f557a9ce7a9d7be8a8cb40002d5cb","0xbd2ec7c608a06fe975dbdca729e84dedb34ecc21","0xc0e30823e5e628df8bc9bf2636a347e1512f0ecb","0x65f9b2e4d7aaeb40ffea8c6f5844d5ad7da257e0","0x36b01066b7fa4a0fdb2968ea0256c848e9135674","0xab11204cfeaccffa63c2d23aef2ea9accdb0a0d5","0x6795cf8eb25585eadc356ae32ac6641016c550f2","0xfbf2173154f7625713be22e0504404ebfe021eae","0x6f803466bcd17f44fa18975bf7c509ba64bf3825","0xead6be34ce315940264519f250d8160f369fa5cd","0xd344539efe31f8b6de983a0cab4fb721fc69c547","0x5ca39c42f4dee3a5ba8fec3ad4902157d48700bf","0x2a048d9a8ffdd239f063b09854976c3049ae659c","0xb8cf411b956b3f9013c1d0ac8c909b086218207c","0x2819c144d5946404c0516b6f817a960db37d4929","0x120a270bbc009644e35f0bb6ab13f95b8199c4ad","0x9e6316f44baeeee5d41a1070516cc5fa47baf227","0x70faa28a6b8d6829a4b1e649d26ec9a2a39ba413","0x563b377a956c80d77a7c613a9343699ad6123911","0xd3273eba07248020bf98a8b560ec1576a612102f","0x3b0bc51ab9de1e5b7b6e34e5b960285805c41736","0xeed16856d551569d134530ee3967ec79995e2051","0x324cc2c9fb379ea7a0d1c0862c3b48ca28d174a4","0x91f6d99b232153cb655ad3e0d05e13ef505f6cd5","0xe855283086fbee485aecf2084345a91424c23954","0xa96b536eef496e21f5432fd258b6f78cf3673f74","0xea3a46bd1dbd0620d80037f70d0bf7c7dc5a837c","0xed8204345a0cf4639d2db61a4877128fe5cf7599","0x3613ef1125a078ef96ffc898c4ec28d73c5b8c52","0x0a73573cf2903d2d8305b1ecb9e9730186a312ae","0x0068eb681ec52dbd9944517d785727310b494575","0xb2cc3cdd53fc9a1aeaf3a68edeba2736238ddc5d","0x1119aaefb02bf12b84d28a5d8ea48ec3c90ef1db","0x2f1233ec3a4930fd95874291db7da9e90dfb2f03","0x390de26d772d2e2005c6d1d24afc902bae37a4bb","0xba826fec90cefdf6706858e5fbafcb27a290fbe0","0x5e032243d507c743b061ef021e2ec7fcc6d3ab89","0xb436c96c6de1f50a160ed307317c275424dbe4f2","0xf5bec430576ff1b82e44ddb5a1c93f6f9d0884f3","0x8f3ab2c3b651382b07a76653d2be9eb4b87e1630","0xd94c9ff168dc6aebf9b6cc86deff54f3fb0afc33","0x42da8a05cb7ed9a43572b5ba1b8f82a0a6e263dc","0x700f6912e5753e91ea3fae877a2374a2db1245d7","0x60d0cc2ae15859f69bf74dadb8ae3bd58434976b")
  //args(2).split(",").map(x=>x.toLowerCase)
  override def setup(): Unit =
    proxy.getVerticesSet().foreach { v =>
      val vertex = proxy.getVertex(v._2)
      val walletID = vertex.getPropertyCurrentValue("id").get.asInstanceOf[String]
      if(walletID equals infectedNode) {
        vertex.getOrSetCompValue("infected", infectionStartingBlock)
        vertex.getOrSetCompValue("infectedBy", "Start")
        vertex.getOutgoingNeighborsAfter(infectionStartingBlock).foreach { neighbour =>
          vertex.messageNeighbour(neighbour._1, (walletID,neighbour._2.getTimeAfter(infectionStartingBlock)))
        }
      }
    }

  override def analyse(): Unit =
    proxy.getVerticesWithMessages().foreach { vert =>
      val vertex = proxy.getVertex(vert._2)
      var infectionBlock = infectionStartingBlock
      var infector = infectedNode
      val queue  = vertex.messageQueue.map(_.asInstanceOf[(String,Long)])
      if (queue.nonEmpty) {
        infectionBlock = queue.map(x=>x._2).min
        infector = queue.filter(x=>x._2==infectionBlock).head._1 //todo check if multiple
        vertex.clearQueue
      }
      //if (vertex.containsCompValue("infected"))
      //  vertex.voteToHalt() //already infected
      //else {
      val walletID = vertex.getPropertyCurrentValue("id").get.asInstanceOf[String]
      if(walletID contains listOfExchanges){
        vertex.getOrSetCompValue("exchangeHitAt", infectionBlock)
        vertex.getOrSetCompValue("exhangeHitBy",infector)
        vertex.getOrSetCompValue("infected", infectionBlock)
        vertex.getOrSetCompValue("infectedBy",infector)
      }
      else{
        vertex.getOrSetCompValue("infected", infectionBlock)
        vertex.getOrSetCompValue("infectedBy",infector)
        vertex.getOutgoingNeighborsAfter(infectionBlock).foreach { neighbour =>
          vertex.messageNeighbour(neighbour._1, (walletID,neighbour._2.getTimeAfter(infectionBlock)))
        }
      }

      //}
    }

  override def returnResults(): Any =
    proxy
      .getVerticesSet()
      .map { vert =>
        val vertex = proxy.getVertex(vert._2)
        if (vertex.containsCompValue("infected"))
          (vertex.getPropertyCurrentValue("id").get.asInstanceOf[String], vertex.getCompValue("infected").asInstanceOf[Long],vertex.getCompValue("infectedBy").asInstanceOf[String])
        else
          ("", -1L,"")

      }
      .filter(f => f._2 >= 0).par

  override def defineMaxSteps(): Int = 100

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParIterable[(String, Long,String)]]].flatten
    //println(s"Run as of ${System.currentTimeMillis()}")
    for (elem <- endResults) {println(s"${elem._1},${elem._2},${elem._3},")}
  }
  override def processViewResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParIterable[(String, Long,String)]]].flatten
    var data = s"{block:$timeStamp,edges:["
    for (elem <- endResults)
      data+=s"""{"infected":"${elem._1}","block":"${elem._2}","infector":"${elem._3}"}"""
    data+="]}"
    publishData(data)
    //for (elem <- endResults) {    Utils.writeLines(s"/Users/mirate/Documents/phd/etheroutput/block${timeStamp}.csv", s"${elem._1},${elem._2},${elem._3},", "")}

  }


}
