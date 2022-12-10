package network

import protos.network.{
  NetworkGrpc,
  Address,
  Range,
  ConnectionRequest,
  ConnectionReply,
  SamplingReply,
  SamplingRequest,
  RangeGenerateRequest,
  RangeGenerateReply,
  ShuffleReadyRequest,
  ShuffleReadyReply,
  ShuffleCompleteRequest,
  ShuffleCompleteReply,
  SortPartitionReply,
  SortPartitionRequest,
  MergeRequest,
  MergeReply,
  ResultType
}
import java.util.logging.Logger
import scala.concurrent.{ExecutionContext, Future}
import io.grpc.{Server, ServerBuilder, Status}
import io.grpc.stub.StreamObserver;
import java.net.InetAddress
import java.net._
import java.io.{OutputStream, FileOutputStream, File}
import scala.sys.process._
import scala.io.Source
import com.google.protobuf.ByteString
import rangegenerator.keyRangeGenerator
import shufflenetwork.FileServer
import common.{WorkerState, WorkerInfo}
import scala.concurrent.duration.Duration

object NetworkServer {
  private val logger =
    Logger.getLogger(classOf[NetworkServer].getName)

  private val port = 50051

  def apply(
      executionContext: ExecutionContext,
      numClients: Int
  ): NetworkServer = {
    new NetworkServer(executionContext, numClients)
  }
}

class NetworkServer(executionContext: ExecutionContext, numClients: Int) {
  self =>
  private[this] var server: Server = null
  private[this] var clientMap: Map[Int, WorkerInfo] = Map()
  private[this] var addressList: Seq[Address] = Seq()
  private[this] var samples: Seq[String] = Seq()
  private[this] var keyRanges:Seq[Range] = Seq()

  private val localhostIP = InetAddress.getLocalHost.getHostAddress

  def start(): Unit = {
    println(localhostIP + ":" + NetworkServer.port)
    server = ServerBuilder
      .forPort(NetworkServer.port)
      .addService(NetworkGrpc.bindService(new NetworkImpl, executionContext))
      .build
      .start
    NetworkServer.logger.info("Server started, listening on " + NetworkServer.port)
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

  /* *** Master's functions *** */
  private class NetworkImpl extends NetworkGrpc.Network {
    override def connection(req: ConnectionRequest): Future[ConnectionReply] = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }

      NetworkServer.logger.info("[Connection] Request from " + addr.ip + ":" + addr.port + " arrived")

      clientMap.synchronized {
        if (clientMap.size < numClients){
          val workerInfo = new WorkerInfo(addr.ip, addr.port)
          clientMap = clientMap + (clientMap.size + 1 -> workerInfo)

          val reply = ConnectionReply(
              result = ResultType.SUCCESS,
              message = "Connection complete to master from " + addr.ip,
              id = clientMap.size
            )
          Future.successful(reply)
        } else{
          NetworkServer.logger.info("[Connection] failed from" + addr.ip)
          Future.failed(new Exception)
        }
      }
    }

    override def sampling(replyObserver: StreamObserver[SamplingReply]) :StreamObserver[SamplingRequest] = {
      new StreamObserver[SamplingRequest]{
        override def onNext(request: SamplingRequest): Unit = {
          val addr = request.addr match {
            case Some(addr) => addr
            case None       => Address(ip = "", port = 1) // TODO: error handling
          }
          NetworkServer.logger.info("[Sampling] gathering samples from"+addr.ip)
          samples = samples ++ request.samples
          val workerInfo = clientMap(request.id)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.Sampling)
            clientMap = clientMap + (request.id -> newWorkerInfo)
          }
          if (addressList.size != numClients) {
          for (i <- 1 to clientMap.size) {
              val workerInfo = clientMap(request.id)
              val address = Address(ip = workerInfo.ip, port = workerInfo.port)
              addressList = addressList :+ address
            }
          }
        }
        override def onError(t: Throwable): Unit = {
          NetworkServer.logger.warning("[Sampling] gathering samples failed.")
          throw t
        }
        override def onCompleted(): Unit = {
          NetworkServer.logger.warning("[Sampling] gathering samples done.")
          val reply = new SamplingReply(ResultType.SUCCESS)
          replyObserver.onNext(reply)
          replyObserver.onCompleted
          keyRanges = new keyRangeGenerator(samples,numClients).generateKeyrange()
        }
      }
    }

    override def rangeGenerate(req:RangeGenerateRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
        NetworkServer.logger.info("[RangeGenerate] ranges are broadcasted")
        val reply = RangeGenerateReply(ResultType.SUCCESS,keyRanges,addressList)
        Future.successful(reply)
    }

    override def sortPartition(req: SortPartitionRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.SortPartition)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (waitWhile(() => !isAllWorkersSameState(WorkerState.SortPartition),100000)
      ) {
        NetworkServer.logger.info("[Sort/Partition] sort/partition completed from " + addr.ip + ":" + addr.port)
        val reply = SortPartitionReply(ResultType.SUCCESS)
        Future.successful(reply)
      } else {
        NetworkServer.logger.info("[Sort/Partition] sort/partition failed from " + addr.ip + ":" + addr.port)
        val reply = SortPartitionReply(ResultType.FAILURE)
        Future.failed(new Exception)
      }
    }

    override def shuffleReady(req: ShuffleReadyRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info("[Shuffle Ready] File Server open Request from " + addr.ip + ":" + addr.port + " arrived")

      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.ShuffleReady)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (waitWhile(() => !isAllWorkersSameState(WorkerState.ShuffleReady),100000)
      ) {
        val reply = ShuffleReadyReply(ResultType.SUCCESS)
        NetworkServer.logger.info("[Shuffle Ready] All shuffle servers are ready to shuffle ")
        Future.successful(reply)
      } else {
        val reply = ShuffleReadyReply(ResultType.FAILURE)
        NetworkServer.logger.info("[Shuffle Ready] shuffle server at" + addr.ip + "is not opend yet.")
        Future.failed(new Exception)
      }
    }

    override def shuffleComplete(req: ShuffleCompleteRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO: error handling
      }
      NetworkServer.logger.info("[Shuffle Complete] Worker " + addr.ip + ":" + addr.port + " completed send partitions")
      clientMap.synchronized {
        for (i <- 1 to clientMap.size) {
          val workerInfo = clientMap(i)
          if (workerInfo.ip == addr.ip && workerInfo.port == addr.port) {
            val newWorkerInfo = new WorkerInfo(addr.ip, addr.port)
            newWorkerInfo.setWorkerState(state = WorkerState.ShuffleComplete)
            clientMap = clientMap + (i -> newWorkerInfo)
          }
        }
      }

      if (waitWhile(() => !isAllWorkersSameState(WorkerState.ShuffleComplete),100000)
      ) {
        val reply = ShuffleCompleteReply(ResultType.SUCCESS)
        NetworkServer.logger.info("[Shuffle Complete] Completed re arrange every items.")
        Future.successful(reply)
      } else {
        val reply = ShuffleCompleteReply(ResultType.FAILURE)
        NetworkServer.logger.info("[Shuffle Complete] shuffle server at" + addr.ip + "is not completed yet.")
        Future.failed(new Exception)
      }
    }

    override def merge(req: MergeRequest) = {
      val addr = req.addr match {
        case Some(addr) => addr
        case None       => Address(ip = "", port = 1) // TODO
      }
      NetworkServer.logger.info("[Merge] Worker " + addr.ip + ":" + addr.port + " completed merge")
      var mergeCompleteWorkers: Int = 0
      mergeCompleteWorkers.synchronized {
        mergeCompleteWorkers += 1
      }
      if (waitWhile(() => mergeCompleteWorkers < numClients, 100000)) {
        val reply = MergeReply(ResultType.SUCCESS)
        NetworkServer.logger.info("[Merge] Completed re arrange every items.")
        Future.successful(reply)
      } else {
        val reply = MergeReply(ResultType.FAILURE)
        NetworkServer.logger.info("[Merge] shuffle server at" + addr.ip + "is not completed yet.")
        Future.failed(new Exception)
      }
    }

    def waitWhile(condition: () => Boolean, timeout: Int): Boolean = {
      for (i <- 1 to timeout / 50)
        if (!condition()) return true else Thread.sleep(50)
      false
    }

    def isAllWorkersSameState(state: WorkerState): Boolean = {
      var res = true
      for (i <- 1 to clientMap.size)
        if (clientMap(i).workerState != state) res = false
      res
    }
  }
}
