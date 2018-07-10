package spark.gensim.scorer

import java.math.MathContext

import spark.gensim.phraser.{PhrasesConfig, Vocab}

object NpmiBigramScorer extends BigramScorer {

  /**
    * Calculation NPMI score based on `"Normalized (Pointwise) Mutual Information in Colocation Extraction"
    * by Gerlof Bouma <https://svn.spraakdata.gu.se/repos/gerlof/pub/www/Docs/npmi-pfd.pdf>`_.
    * @param config - phraser config. (contains params like minCount, threshold etc. not used in this scorer )
    * @param vocab - corpus vocab learnt
    * @param corpus_word_count - total corpus words count (not used in this scorer)
    * @param worda - bigram first token
    * @param wordb - bigram second token
    * @param bigram - bigram to score
    * @return npmi score for bigram [log(pab/(pa*pb)) / -log(pab)]
    */
  override def score(config: PhrasesConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val worda_count = vocab.getCount(worda).toFloat
    val wordb_count = vocab.getCount(wordb).toFloat
    val bigram_count = vocab.getCount(bigram).toFloat

    val pa = worda_count / corpus_word_count
    val pb = wordb_count / corpus_word_count
    val pab = bigram_count / corpus_word_count

    // numerator = log(pab/(pa*pb))
    val numerator = BigDecimal.binary(Math.log(pab/(pa*pb)), MathContext.DECIMAL64)

    // denominator = -log(pab)
    val denominator = -1 * BigDecimal.binary(Math.log(pab.toDouble), MathContext.DECIMAL64)

    // final score
    val res = numerator/denominator
    return res.toDouble
  }
}