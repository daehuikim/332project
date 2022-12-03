package shufflenetwork

import shuffle.shuffle.{
    ShuffleNetworkGrpc,
    SendPartitionRequest,
    SendPartitionReply
}

import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder, Status}
import java.net.InetAddress

object FileServer{
    private val logger = Logger.getLogger(classOf[FileServer].getName)
    private val port = 9000 /*TBD*/
    def apply(
      executionContext: ExecutionContext,
      numClients: Int
    ): FileServer = {
    new FileServer(executionContext, numClients)
  }
}

class FileServer(executionContext: ExecutionContext, numClients: Int) {
    self => 
        private[this] var server: Server = null
        private[this] var clientSet: Map[Int, (String, Int)] = Map()
    private val localhostIP = InetAddress.getLocalHost.getHostAddress

    def start(): Unit = {
        println(localhostIP + ":" + FileServer.port)
        server = ServerBuilder
        .forPort(FileServer.port)
        .addService(ShuffleNetworkGrpc.bindService(new ShuffleNetworkImpl, executionContext))
        .build
        .start
    FileServer.logger.info("File Server started, listening on " + FileServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
        }
    }

    def stop(): Unit = {
        if (server != null) {
        server.shutdown()
        }
    }

    def blockUntilShutdown(): Unit = {
        if (server != null) {
        server.awaitTermination()
        }
    }

    private class ShuffleNetworkImpl extends ShuffleNetworkGrpc.ShuffleNetwork {
        override def sendPartition(req: SendPartitionRequest) = ???
    }
}
