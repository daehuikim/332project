package common

import java.io.File
import scala.collection.mutable.Buffer
import scala.io.Source
import shuffle.shuffle.partitionFile
import protos.network.RangeGenerateReply
import util.control.Breaks.{breakable,break}

object Utils {
  def comparator(s1: String, s2: String): Boolean = {
    s1.slice(0, 10) <= s2.slice(0, 10)
  }
  def comparatorWithout(s1: String, s2: String): Boolean = {
    s1.slice(0, 10) < s2.slice(0, 10)
  }

  def createdir (path:String) = {
    val dir = new File(path)
    if(!dir.exists()){
        dir.mkdir()
    }
  }

  def getFilePathsFromDir(dirs: List[String]): List[String] = {
    val filePaths = dirs.flatMap { dirPath =>
      val dir = new File(dirPath)
      val files = if (dir.exists() && dir.isDirectory()) {
        dir.listFiles().filter(_.isFile()).map(file => file.getPath()).toList
      } else {
        List[String]()
      }
      files
    }
    filePaths
  }

  def getFile(filename: String): Seq[String] = {
    var partition:Buffer[String]=Buffer()
    for (line <- Source.fromFile(filename).getLines())
    {
        partition = partition:+line
    }
    partition
  }

  def getPartitionFile(filename: String): partitionFile = {
    var partition:Seq[String] = Seq()
    for (line <- Source.fromFile(filename).getLines())
    {
        partition = partition:+line
    }
    val result =  new partitionFile(partition)
    result
  }

  def getId(rangeGenerateReply: RangeGenerateReply, localhostIP: String):Int = {
    var id = 0
    breakable{
      for (i <- 0 to rangeGenerateReply.addresses.length-1){
        if(rangeGenerateReply.addresses(i).ip == localhostIP){
          id = i
        }
     }
    }
    id
  }

  def waitWhile(condition: () => Boolean, timeout: Int): Boolean = {
    for (i <- 1 to timeout / 50)
      if (!condition()) return true else Thread.sleep(50)
        false
  }
  
  def convertToimmutable[T](sq: Seq[T]): scala.collection.Seq[T] = scala.collection.Seq[T](sq: _*)
  def convertTomutable[T](sq: scala.collection.Seq[T]): Seq[T] = Seq[T](sq: _*)
}

