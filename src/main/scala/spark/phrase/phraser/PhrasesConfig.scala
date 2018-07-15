package spark.phrase.phraser

import scala.collection.mutable

trait PhrasesConfig extends Serializable {

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

case class SimplePhrasesConfig(
                                minCount:Int = 5,
                                threshold: Float = 10.0F,
                                maxVocabSize: Int = 40000000,
                                delimiter: String = "_",
                                progressPer: Int =50,
                                var commonWords: Option[mutable.HashSet[String]] = None) extends PhrasesConfig

object DefaultPhrasesConfig extends SimplePhrasesConfig()//commonWords = Some(Util.loadStopWords()))