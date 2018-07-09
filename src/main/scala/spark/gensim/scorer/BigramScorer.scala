package spark.gensim.scorer

import java.math.MathContext

import spark.gensim.phraser.{PhraserConfig, Vocab}

trait BigramScorer extends Serializable {
  def score(config: PhraserConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double
}

object BigramScorer {
  val DEFAULT = "default"
  val NPMI = "npmi"
  val CHI_SQ = "chisq"
  val LOG_LIKELIHOOD = "log_likelihood"
  val JACCARD = "jaccard"
  def getScorer(scorerType: String): BigramScorer = {
    return scorerType match {
      case NPMI => NpmiBigramScorer
      case CHI_SQ => ChiSqBigramScorer
      case LOG_LIKELIHOOD => LogLikelyhoodBigramScorer
      case JACCARD => JaccardBigramScorer
      case DEFAULT => DefaultBigramScorer
      case _ => throw new RuntimeException("No matching scorer found for type " + scorerType)
    }
  }
}

// original gensim scorer
object DefaultBigramScorer extends BigramScorer {
  /*
    Bigram scoring function, based on the original `Mikolov, et. al: "Distributed Representations
    of Words and Phrases and their Compositionality" <https://arxiv.org/abs/1310.4546>`_.
*/
  override def score(config: PhraserConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val worda_count = vocab.getCount(worda).toDouble
    val wordb_count = vocab.getCount(wordb).toDouble
    val bigram_count = vocab.getCount(bigram).toDouble
    val min_count = config.minCount.toDouble
    val vocab_len = vocab.size().toDouble

    val a = BigDecimal.binary((bigram_count - min_count), MathContext.DECIMAL64) / BigDecimal.binary(worda_count, MathContext.DECIMAL64) / BigDecimal.binary(wordb_count, MathContext.DECIMAL64) * vocab_len
    a.toDouble
  }
}