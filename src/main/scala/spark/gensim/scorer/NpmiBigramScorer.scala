package spark.gensim.scorer

import java.math.MathContext

import spark.gensim.phraser.{PhrasesConfig, Vocab}

object NpmiBigramScorer extends BigramScorer {

  /*
  """Calculation NPMI score based on `"Normalized (Pointwise) Mutual Information in Colocation Extraction"
  by Gerlof Bouma <https://svn.spraakdata.gu.se/repos/gerlof/pub/www/Docs/npmi-pfd.pdf>`_.
  */
  override def score(config: PhrasesConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
//    val worda_count = BigDecimal.binary(vocab.getCount(worda).toDouble, MathContext.DECIMAL64)
//    val wordb_count = BigDecimal.binary(vocab.getCount(wordb).toDouble, MathContext.DECIMAL64)
//    val bigram_count = BigDecimal.binary(vocab.getCount(bigram).toDouble, MathContext.DECIMAL64)
    val worda_count = vocab.getCount(worda).toFloat
    val wordb_count = vocab.getCount(wordb).toFloat
    val bigram_count = vocab.getCount(bigram).toFloat

    val pa = worda_count / corpus_word_count
    val pb = wordb_count / corpus_word_count
    val pab = bigram_count / corpus_word_count
    val numerator = BigDecimal.binary(Math.log(pab/(pa*pb)), MathContext.DECIMAL64)
    val denominator = -1 * BigDecimal.binary(Math.log(pab.toDouble), MathContext.DECIMAL64)
    // return (Math.log(numerator.toDouble) / (-1 * Math.log(pab.toDouble)))
    val res = numerator/denominator
    return res.toDouble
  }
}