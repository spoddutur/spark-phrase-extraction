package spark.gensim.scorer

import spark.gensim.phraser.{PhrasesConfig, Vocab}

import scala.collection.mutable.ListBuffer

object LogLikelyhoodBigramScorer extends ContingencyBasedBigramScorer {

  private val SMALL = 1e-20

  /**
    * Use marginals of bigram to compute loglikelyhood
    * @param config - phraser config. (contains params like minCount, threshold etc. not used in this scorer )
    * @param vocab - corpus vocab learnt
    * @param corpus_word_count - total corpus words count (not used in this scorer)
    * @param worda - bigram first token
    * @param wordb - bigram second token
    * @param bigram - bigram to score
    * @return loglikelyhood score for bigram [log(pab/(pa*pb)) / -log(pab)]
    */
  override def score(config: PhrasesConfig, vocab: Vocab, corpus_word_count: Int, worda: String, wordb: String, bigram: String): Double = {
    val (n_ii, n_ix, n_xi, n_xx) = marginals(vocab, worda, wordb, bigram)
    likelyhood_ratio(n_ii, n_ix, n_xi, n_xx).toFloat
  }

  def likelyhood_ratio(n_ii: Int, n_ix: Int, n_xi: Int, n_xx: Int): Double = {

    val (n_ii_c, n_oi, n_io, n_oo) = contingency(n_ii, n_xi, n_ix, n_xx)
    val cont = new ListBuffer[Int]()
    cont += n_ii
    cont += n_oi
    cont += n_io
    cont += n_oo
    val n_e = expectedValues(n_ii, n_oi, n_io, n_oo)
    val exp = new ListBuffer[Float]()
    exp += n_e._1
    exp += n_e._2
    exp += n_e._3
    exp += n_e._4
    var out = 0.0d
    for(i <- 0 to 3) {
      out = out + (cont(i) * Math.log(cont(i).toFloat / (exp(i) + SMALL) + SMALL))
    }
    out
  }
}


