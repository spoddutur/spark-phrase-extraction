package spark.gensim.common

import scala.collection.mutable

trait PhraserConfig extends Serializable {

  val minCount: Int
  val threshold: Float
  val maxVocabSize: Int
  val delimiter: String
  val progressPer: Int
  var commonWords: Option[mutable.HashSet[String]]

  def isCommonWord(word: String): Boolean = {
    word != null && word != "" && commonWords.isDefined && commonWords.get.contains(word)
  }
}

case class SimplePhraserConfig(
                                minCount:Int = 5,
                                threshold: Float = 10.0F,
                                maxVocabSize: Int = 40000000,
                                delimiter: String = "_",
                                progressPer: Int =50,
                                var commonWords: Option[mutable.HashSet[String]] = None) extends PhraserConfig

object DefaultPhraserConfig extends SimplePhraserConfig()//commonWords = Some(Util.loadStopWords()))