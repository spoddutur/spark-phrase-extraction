package spark.gensim.score
import spark.gensim.common.{PhraserConfig, Vocab}

object JaccardBigramScorer extends ContingencyBasedBigramScorer {

  override def score(config: PhraserConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val (n_ii, n_ix, n_xi, n_xx) = marginals(vocab, worda, wordb, bigram)
    jaccard(n_ii, n_ix, n_xi, n_xx)
  }

  def jaccard(n_ii: Int, n_ix: Int, n_xi: Int, n_xx: Int): Double = {
    // """Scores ngrams using the Jaccard index."""
    val (n_ii_c, n_oi, n_io, n_oo) = contingency(n_ii, n_xi, n_ix, n_xx)
    return (n_ii) / (n_ii + n_oi + n_io)
  }
}
