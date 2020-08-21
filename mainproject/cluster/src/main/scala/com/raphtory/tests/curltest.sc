val nodeIP       = System.getenv().getOrDefault("SPOUT_ETHEREUM_IP_ADDRESS", "54.184.149.221").trim
val nodePort     = System.getenv().getOrDefault("SPOUT_ETHEREUM_PORT", "8545").trim
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.utils.Utils
import com.raphtory.tests.EtherAPITest.baseRequest
import com.raphtory.tests.EtherAPITest.currentBlock
import com.raphtory.tests.EtherAPITest.request

import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration.NANOSECONDS
import scala.concurrent.duration.SECONDS
import scala.language.postfixOps
import scala.sys.process._
import scalaj.http.Http
import scalaj.http.HttpRequest
import spray.json._
import java.net.InetAddress
import scala.collection.mutable.ArrayBuffer
val currentBlock = 9000000

def hostname2Ip(hostname: String): String = InetAddress.getByName(hostname).getHostAddress()

def requestBuilder() =
  if (nodeIP.matches(Utils.IPRegex))
    Http("http://" + nodeIP + ":" + nodePort).header("content-type", "application/json")
  else
    Http("http://" + hostname2Ip(nodeIP) + ":" + nodePort).header("content-type", "application/json")

val baseRequest  = requestBuilder()

def requestBatch(data: String): HttpRequest = baseRequest.postData(data)

def executeBatchRequest(data: String) = requestBatch(data).execute().body.toString.parseJson

def request(command: String, params: String = ""): HttpRequest =
  baseRequest.postData(s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}""")
def executeRequest(command: String, params: String = "") =
  request(command, params).execute().body.toString.parseJson.asJsObject

def batchRequestBuilder(command:String,params:String):String = s"""{"jsonrpc": "2.0", "id":"100", "method": "$command", "params": [$params]}"""

def pullNextBlock(): Unit = {
    val transactionCountHex = executeRequest("eth_getBlockTransactionCountByNumber", "\"0x" + currentBlock.toHexString + "\"")
    val transactionCount = Integer.parseInt(transactionCountHex.fields("result").toString().drop(3).dropRight(1), 16)
    var transactions = "["
    for (i <- 0 until transactionCount)
        transactions = transactions + batchRequestBuilder("eth_getTransactionByBlockNumberAndIndex",s""""0x${currentBlock.toHexString}","0x${i.toHexString}"""")+","
  println(executeBatchRequest(transactions.dropRight(1)+"]"))
}
pullNextBlock()