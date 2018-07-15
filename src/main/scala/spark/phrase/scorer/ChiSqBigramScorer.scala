package spark.phrase.scorer

import spark.phrase.phraser.{PhrasesConfig, Vocab}

object ChiSqBigramScorer extends ContingencyBasedBigramScorer {

  private val SMALL = 1e-20

  override def score(config: PhrasesConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val (n_ii, n_ix, n_xi, n_xx) = marginals(vocab, worda, wordb, bigram)
    chiSq(n_ii, n_ix, n_xi, n_xx).toFloat
  }

  def phiSq(n_ii: Int, n_oi: Int, n_io: Int, n_oo: Int): Double = {
    val numerator = ((n_ii * n_oo) - (n_io * n_oi))
    val denominator = (n_ii + n_io) * ((n_ii + n_oi) * (n_oo + n_io) * (n_oo + n_oi))
    return math.pow(numerator, 2) / (denominator)
  }

  def chiSq(n_ii: Int, n_ix: Int, n_xi: Int, n_xx: Int): Double = {
    return n_xx * phiSq(n_ii, n_ix, n_xi, n_xx)
  }
}
