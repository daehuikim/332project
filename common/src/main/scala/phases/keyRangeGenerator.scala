package rangegenerator

import scala.collection.mutable.Buffer
import protos.network.Range
import common.Utils
import com.google.protobuf.ByteString

class keyRangeGenerator(
    allSamples: scala.collection.Seq[ByteString],
    numWorkers: Int
) {
  val outputSize = 1000000 // 100MB
  val temp = allSamples.map(x => x.toStringUtf8)
  val sortedSamples = temp.sortWith((s1, s2) => Utils.comparator(s1, s2))
  
  // val sortedSamples = Utils.convertToimmutable(sortedSample)
  // val sortedSamples = sortedSample.map(x:ByteString => x.toString("UTF-8"))

  def generateKeyrange(): Seq[Range] = {
    val numPoints = numWorkers - 1
    val term = sortedSamples.length / numWorkers
    val remain = sortedSamples.length % numWorkers

    var points: Buffer[Int] = Buffer()
    for (i <- 0 to remain - 1) {
      points = points :+ ((term + 1) * (i + 1))
    }
    for (i <- remain * (term + 1) + term to sortedSamples.length - 1 by term) {
      points = points :+ i
    }

    println("sortedSamples")
    sortedSamples.foreach(println)

    println("points")
    points.foreach(println)
    println("points length",points.length.toString())

    var ranges: Buffer[Range] = Buffer()
    for (i <- 0 to points.length - 1) {
      println("i value",i.toString())
      if (i == 0) {
        println("in if")
        var el = Range("          ", sortedSamples(points.head))
        ranges = ranges :+ el
      } else {
        println("in else")
        var el = Range(sortedSamples(points(i-1)), sortedSamples(points(i)))
        ranges = ranges :+ el
      }
    }
    ranges :+ Range(sortedSamples(points.last), "~~~~~~~~~~")
  }
}
