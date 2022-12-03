package network

import protos.network.{
  NetworkGrpc,
  Address,
  ConnectionRequest,
  ConnectionReply,
  MergeRequest,
  MergeReply,
  SortPartitionReply,
  SortPartitionRequest,
  SamplingReply,
  SamplingRequest,
  FileRequest,
  FileReply,
  ShuffleReadyRequest,
  ShuffleReadyReply,
  ShuffleCompleteRequest,
  ShuffleCompleteReply,
  ResultType
}

import shuffle.shuffle.ShuffleNetworkGrpc.ShuffleNetworkBlockingStub
import protos.network.NetworkGrpc.{NetworkBlockingStub, NetworkStub}
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}
import io.grpc.stub.StreamObserver
import io.grpc.{Status}

import java.net.InetAddress
import java.io.{OutputStream, FileOutputStream, File, IOException}
import java.nio.file.{Files, Path, Paths}
import scala.concurrent.{Promise, Await}
import scala.concurrent.duration._

import shufflenetwork.FileServer

object NetworkClient {
  def apply(host: String, port: Int): NetworkClient = {
    val channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = NetworkGrpc.blockingStub(channel)
    new NetworkClient(channel, blockingStub)
  }
}

class NetworkClient private (
    private val channel: ManagedChannel,
    private val blockingStub: NetworkBlockingStub
) {
  val id: Int = -1
  val localhostIP = InetAddress.getLocalHost.getHostAddress
  val port = 8000

  private[this] val logger =
    Logger.getLogger(classOf[NetworkClient].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def connect(address: String): ConnectionReply = {
    logger.info("[Connection]: Start to connect to Master server " + address)

    val addr = Address(localhostIP, port)
    val request = ConnectionRequest(Some(addr))
    try {
      val response = blockingStub.connection(request)
      logger.info("[Connection]: " + response.message)

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        ConnectionReply(ResultType.FAILURE)
    }
  }

  /*Sample phase: send samples*/
  final def sendSamples(samples: Seq[String]): SamplingReply = {
    logger.info("[Sampling] Try to send samples to Master")
    val addr = Address(localhostIP, port)
    val request = SamplingRequest(Some(addr), samples)

    try {
      val response = blockingStub.sampling(request)
      logger.info(
        "[Sampling] Received sampling response from Master"
      )
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        SamplingReply(ResultType.FAILURE)
    }
  }

  def checkShuffleReady(): ShuffleReadyReply ={
    logger.info("[Shuffle] Try to send Shuffle ready to Master")
    val addr = Address(localhostIP, port)
    val request = ShuffleReadyRequest(Some(addr))
    println(request)
    try {
      val response = blockingStub.shuffleReady(request)
      logger.info("[shuffle]: Connect" + response.result)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        ShuffleReadyReply(ResultType.FAILURE)
    }
  }

  def sortPartitionComplete(): SortPartitionReply = {
    logger.info(
      "[Sort/Partition]: Try to send finish message to Master server"
    )

    val addr = Address(localhostIP, port)
    val request = SortPartitionRequest(Some(addr))
    try {
      val response = blockingStub.sortPartition(request)
      logger.info("[Sort/Partition]: " + response.message)

      response
    } catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
        SortPartitionReply("failed")
    }
  }
}
