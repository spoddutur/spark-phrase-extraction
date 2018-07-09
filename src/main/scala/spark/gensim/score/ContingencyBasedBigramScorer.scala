package spark.gensim.score

import spark.gensim.common.Vocab

import scala.collection.mutable.ListBuffer

trait ContingencyBasedBigramScorer  extends BigramScorer {

  /**
    *
    * Bigram here can contain multiple words. So, taking in worda, wordb and bigram separately.
    * @param vocab corpus
    * @param worda "bank_of"
    * @param wordb "america"
    * @param bigram "bank_of_america"
    * @return
    */
  def contingency(vocab: Vocab, worda: String, wordb: String, bigram: String): (Int, Int, Int, Int) = {

    val delimiter = vocab.delimiter
    val n_ii = vocab.wordCounts.getOrElse(bigram, 0)
    var n_io = 0
    var n_oi = 0
    var n_oo = 0
    vocab.wordCounts.map( wordCount => {
      val word = wordCount._1
      val count = wordCount._2
      val isUnigram = word.contains(delimiter)
      if(!isUnigram) {
        val startsWithWorda = word.startsWith(worda + delimiter)
        val endsWithWordb = word.endsWith(delimiter + wordb)
        n_oo = if (!startsWithWorda && !endsWithWordb) n_oo + count else n_oo
        n_io = if (startsWithWorda && !endsWithWordb) n_io + count else n_io
        n_oi = if (!startsWithWorda && endsWithWordb) n_oi + count else n_oi
      }
    })
    (n_ii, n_oi, n_io, n_oo)
  }

  def marginals(vocab: Vocab, worda: String, wordb: String, bigram: String): (Int, Int, Int, Int) = {
    val (n_ii, n_oi, n_io, n_oo) = contingency(vocab, worda, wordb, bigram)
    val n_xi = (n_ii + n_oi)
    val n_ix = (n_ii + n_io)
    val n_xx = (n_ii + n_oi + n_io + n_oo)
    (n_ii, n_ix, n_xi, n_xx)
  }

  def contingency(n_ii: Int, n_ix: Int, n_xi: Int, n_xx: Int): (Int, Int, Int, Int) = {

    val n_oi = n_xi - n_ii
    val n_io = n_ix - n_ii
    val n_oo = n_xx - n_ii - n_io - n_oi
    (n_ii, n_oi, n_io, n_oo)
  }

  def expectedValues(n_ii: Int, n_oi: Int, n_io: Int, n_oo: Int): (Float, Float, Float, Float) = {
    val cont = Seq(n_ii, n_oi, n_io, n_oo)
    val n_xx = (n_ii + n_oi + n_io + n_oo).toFloat
    val n_e: ListBuffer[Float] = new ListBuffer[Float]
    for(i <- 0 to 3) {
      n_e += (cont(i) + cont(i ^ 1)) * (cont(i) + cont(i ^ 2)) / n_xx
    }
    (n_e(0), n_e(1), n_e(2), n_e(3))
  }
}
