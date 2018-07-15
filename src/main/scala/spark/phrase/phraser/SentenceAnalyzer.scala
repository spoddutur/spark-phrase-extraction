package spark.phrase.phraser

import spark.phrase.scorer.BigramScorer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait SentenceAnalyzer {

  def analyze(sentence: Phraser.SENTENCE_TYPE, phrases_model: Phrases, scorer: BigramScorer):  mutable.ListBuffer[(String, Option[Double])] = {

    var out: mutable.ListBuffer[(String, Option[Double])] = new mutable.ListBuffer[(String, Option[Double])]()
    var words: Array[Option[String]] = sentence.map(x => Some(x))
    words = words :+ None
    var last_uncommon_word: Option[String] = None
    var in_between_words: Option[ListBuffer[String]] = None
    for(optionWord <- words) {
      val word = if(optionWord.isDefined) optionWord.get else ""
      val is_not_common = !phrases_model.config.isCommonWord(word)
      if (is_not_common && last_uncommon_word.isDefined) {
        var chain = Seq(last_uncommon_word.get)
        if(in_between_words.isDefined) {
          chain = chain ++ in_between_words.get
        }
        chain = chain ++ Seq(word)
        val score = scoreItem(last_uncommon_word.get, word, chain, phrases_model, scorer)
        if(score > phrases_model.config.threshold) {
          out += Tuple2(chain.mkString(phrases_model.config.delimiter), Some(score))
          last_uncommon_word = None
          in_between_words = None
        } else {
          // release each token individually
          for(token <- chain) {
            if(token.size > 0 && !token.equals(word)) {
              out += Tuple2(token, None)
            }
          }
          last_uncommon_word = Some(word)
          in_between_words = None
        }
      } else if(is_not_common) {
        last_uncommon_word = Some(word)
      } else { // common
        if(last_uncommon_word.isDefined) {
          // wait for uncommon resolution
          if(in_between_words.isDefined) {
            in_between_words.get += word
          } else {
            in_between_words = Some(ListBuffer[String](word))
          }
        } else{
          out += Tuple2(word, None)
        }
      }
    }
    out
  }

  def scoreItem(worda: String, wordb: String, components: Seq[String], phrases_model: Phrases, scorer: BigramScorer): Double
}

object DefaultSentenceAnalyzer extends Serializable with SentenceAnalyzer {

  override def scoreItem(worda: String, wordb: String, components: Seq[String], phrases_model: Phrases, scorer: BigramScorer): Double = {

    val vocab = phrases_model.corpus_vocab
    if (vocab.contains(worda) && vocab.contains(wordb)) {
      if (vocab.contains(components.mkString(vocab.delimiter))) {
        return scorer.score(phrases_model.config, vocab, phrases_model.corpus_word_count, worda, wordb, components.mkString(vocab.delimiter))
      }
    }
    return -1
  }
}
