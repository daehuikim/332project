package shufflenetwork

import shuffle.shuffle.{
    sAddress,
    sResultType,
    ShuffleNetworkGrpc,
    SendPartitionRequest,
    SendPartitionReply
}
import io.grpc.{ManagedChannel,StatusRuntimeException,ManagedChannelBuilder}
import java.net.InetAddress
import shuffle.shuffle.ShuffleNetworkGrpc.ShuffleNetworkBlockingStub
import java.util.logging.{Level, Logger}
import java.util.concurrent.TimeUnit


object FileClient{
def apply(host: String, port: Int): FileClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = ShuffleNetworkGrpc.blockingStub(channel)
    new FileClient(channel, blockingStub)
  }
    
}

class FileClient(
    private val channel: ManagedChannel,
    private val blockingStub: ShuffleNetworkBlockingStub
) {
  val id: Int = -1
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 8000

  private[this] val logger =
    Logger.getLogger(classOf[FileClient].getName)

    def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }
  
  def getLine(to: String): String = {
    val inputFile = Source.fromFile("./data/input/" + to)
    var lines =
      for {
        line <- inputFile.getLines
      } yield line // type of lines = <iterator>

    var copyInput = lines.mkString("\n")
    assert(!copyInput.isEmpty())
    copyInput
  }

  def sendPartitions(to: String): SendPartitionReply = {
    logger.info("[Shuffle] Try to send partition to" + to)
    val fromaddr = sAddress(localhostIP, port)
    val toaddr = sAddress(to, port)
    val partition = getLine(to)
    val request = SendPartitionRequest(Some(fromaddr), Some(toaddr), partition)

    try{
        val response = blockingStub.sendPartition(request)
        logger.info("[Shuffle] Sent"+request.partition+"from" + localhostIP + "to"  + to)
        response
    }catch{
        case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        SendPartitionReply(sResultType.FAILURE)
    }
  }
}
