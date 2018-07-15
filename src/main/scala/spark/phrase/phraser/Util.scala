package spark.phrase.phraser

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.io.Source

class Util {}
object Util {

  def loadStopWords(): mutable.HashSet[String] = {
    val fileStream = getClass.getResourceAsStream("/stopwords.txt")
    val stopWords = mutable.HashSet[String]()
    Source.fromInputStream(fileStream).getLines().foreach(stopWords.add)
    stopWords
  }

  def save[T](obj: T, outFile: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(outFile))
    oos.writeObject(obj)
    oos.close
  }

  def load[T](filePath: String): T = {
    val ois = new ObjectInputStream(new FileInputStream(filePath))
    val obj = ois.readObject.asInstanceOf[T]
    ois.close
    obj
  }

  def main(args: Array[String]): Unit = {
    loadStopWords().foreach(println)
  }

}
