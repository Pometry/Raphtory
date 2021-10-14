package com.raphtory.dev.algorithms

import com.raphtory.algorithms.old.Analyser

import scala.io.Source
import scala.tools.nsc.io.Path

/**
  * Gets neighbors of node n.
**/
class getWordHash(args: Array[String]) extends Analyser[Any](args) {
  val wl = "jondesign, 1672606215\nsscotland, -1818083225\npachanga, 1038603673\nsteelehawker, 822472666\nticketshopnewsreviewssingles, -668210326\noxfordshirewoking, -1164168711\ndilind, 1346384260\nservicesfunfair, 448931754\ntigerzine, 1406801148\njacketstopman, 894232168\ngardism, 444133964\nardic, 1956919061\nbanx, -46657951\nchavmeisterdeluxe, 869723261\nlongnan, 336246665\nloiku, 1730527558\npermaset, 1343327505\nlabrok, 1498748425\niilarks, -695600456\nrecordability, -608844310\nchambres, -1147734080\nparkhead, 151700222\npotiche, 1099632174\nbluedream, 1983442905\nerbetig, -732738225\nmagarita, 672586443\ntaylormartin, -2102268968\nfabelo, 2018743045\nprintoki, -1861765164\nsholazar, -1349599410\nclubsnowsports, 19023289\nalfter, 1203000663\nizabo, -638123560\nportata, 116278837\nupadana, 1115455928\nviagpure, -1987243705\nmmidlands, -808336510\nsejf, -488759392\nmistrza, -1040600376\nbulgarianestatesdirect, 1417058885\nnevo, 1268297701\ndumonciau, -1320795879\nhurleylouise, 544261975\nseabien, -706238619\nethnomusicologist, -1793727768\nfonterutoli, -1529575842\nnovocastrians, 493720005\nappendchild, -390893090\nactsumbrellasveilsvenue, -1808971376\nciett, -1096811262\nyaoo, -334098758\npinnafore, 1010044314\ngreenlink, 440555331\nmizarolli, -395914840\ndescodificador, -1791406270\nnicover, 1737852194\nsarenza, -1939931930\njwm, -1355078709\ncarabou, -1170704954\ntorquesteer, 2104258196\ncharterwood, 1622316563\nbaumg, 1069402472\nrosskamp, 696847720\nkaws, 1371074937\nhuyswarburg, 705311629\nreturnsaliens, -1824155535\nsupacompare, -710779909\nideasuncle, -1680425491\nsilverfoxnik, 385619805\ntaboos, -1062561109\npaeonies, 17969862\nmuslimtruthrevealed, 882708408\nmakersfood, 1535135962\ndoldu, 1458001564\nshaolinspur, 1711283114\nmaspoma, 1003786300\nsummar, 2124516381\ninterfered, 659437032\nsizlerin, 212491384\nchargeannual, -1058150535\noblivian, 505017834\necosy, 358542502\nstavri, -105562263\nideall, -108752390\nzonecoles, -1796170067\nxant, -3574112\ndigartrefedd, 1578378687\neveyone, -183969044\nloik, 667412496\nfinisheswerzalit, 1687732647\ndringend, -120791795\nsilentwitness, -53805886\noptionalemail, -1456971149\nsportspy, -1524427492\nprevedere, 1418035494\nreoviridae, 1083938124\nhanshing, -52786704\nhoyvoy, -772874953\ngqview, -1283629974\nmacgames, 1751439403\nukwinegifts, -2136371606\nwebrtc, 517643633\nbijanbill, -845680737\nglobemark, -1390055182\naacc, -554274915\nvisionics, -1744963527\nppcline, -1442952419\naccommodationhotelguest, -461452601\ntopgear, -806970492\nsubmertec, 214878248\nrehabilitationtermination, -1660817988\nlendi, 1319064067\ndiffiniti, 1212787142\ncompeling, 26319415\nazinphos, 1577509337\ntrustsetfsisainvestment, 1326981283\nklokken, -592951339\ngeneralcookie, 395282273\nthortons, 1655285227\ninstructorzone, 2069006614\ngllightfv, -698879798\nskandi, -278934135\nreceptes, -19022880\nnridtkvtutfbsditfpsfwjfxt, -338132392\nledc, 1683728049\nnetgnostics, 1487242306\nhiregames, -1503219835\nbirdparti, 1795029116\ndiaconu, 1817959020\ndancebuzz, 58813157\nideasuktv, -1130389565\nsteamstats, 1605170885\nunbeatables, 1320225796\nseramis, -177140960\nielectribe, -1516751418\nkumarasinghe, 1703318016\ncachondas, 1553450147\nbiggirls, 2118307183\nmorios, 1156632842\nmanray, 301086032\nrecoba, 1893199968\nbussman, -1698172400\ngnosis, 860861083\ncpit, 1731118274\nnaega, -479451895\ntyrkmenistan, -32559532\nvalcarcelas, 1271559235\nincludeblogs, 1714474366\nsauteuses, -1067943940\nattma, 1061645496\nemyvale, 990876145\nyourecho, 470832692\nphotoquest, 1354214105\nozq, 1725527632\ndxdsdsq, -111470539\npoolroute, 1052542269\ndmsg, 1743387067\npornfass, -1545816670\nradex, -759819383\nsharked, 1863113741\nhandd, -1619361849\nsnasw, -1116346884\nacolo, 914993499\nenzymetic, -1789371734\nurmacher, 851091343\ncysylltau, 409363014\ngaishan, -1470534926\neverymorning, -1612533171\nrewirescarpet, 112251744\nselectaquafxaquarianaquarium, -1580537091\ntrashcan, -1910143993\ncyel, -142791055\nefoundations, -439783183"
   .split("\n")
  println(wl.length)
  val wds= (if (args.length < 1) wl else dllCommFile(args.head))
  .map(_.split(",")).map(x=> (x.last.trim.toLong, x(2))).toMap
  val output_file: String  = System.getenv().getOrDefault("OUTPUT_PATH", "").trim

  def dllCommFile(url: String): Array[String] = {
    val html = if (url.startsWith("http")) Source.fromURL(url) else Source.fromFile(url)
    html.mkString.split("\n")
  }

  override def setup(): Unit = {  }

  override def analyse(): Unit = {  }

  override def returnResults(): Any = {
    view
      .getVertices()
      .filter(v => wds.keys.toArray.contains(v.ID())).toList
      .map{v=> (v.ID(), v.getPropertyValue("Word").getOrElse(v.ID()).toString, wds.getOrElse(v.ID(), "Unknown"))}
  }

  override def defineMaxSteps(): Int = 10

  override def extractResults(results: List[Any]): Map[String, Any] = {
    val endResults = results
      .asInstanceOf[List[List[(Long, String,String)]]]
      .flatten
    val text =
      s"""${endResults.map(x=> s"${x._1},${x._2},${x._3}").mkString("\n")}"""
    output_file match {
      case "" => println(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
    Map[String, Any]()
  }
}