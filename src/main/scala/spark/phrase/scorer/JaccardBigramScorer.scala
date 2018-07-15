package spark.phrase.scorer
import spark.phrase.phraser.{PhrasesConfig, Vocab}

object JaccardBigramScorer extends ContingencyBasedBigramScorer {

  /**
    * Get marginals and use them to compute marginals
    * @param config - phraser config. (contains params like minCount, threshold etc. not used in this scorer )
    * @param vocab - corpus vocab learnt
    * @param corpus_word_count - total corpus words count (not used in this scorer)
    * @param worda - bigram first token
    * @param wordb - bigram second token
    * @param bigram - bigram to score
    * @return score based on jaccard index
    */
  override def score(config: PhrasesConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val (n_ii, n_ix, n_xi, n_xx) = marginals(vocab, worda, wordb, bigram)
    jaccard(n_ii, n_ix, n_xi, n_xx)
  }

  /*
   Scores ngrams using the Jaccard index.
   */
  def jaccard(n_ii: Int, n_ix: Int, n_xi: Int, n_xx: Int): Double = {
    val (n_ii_c, n_oi, n_io, n_oo) = contingency(n_ii, n_xi, n_ix, n_xx)
    return (n_ii) / (n_ii + n_oi + n_io)
  }
}
