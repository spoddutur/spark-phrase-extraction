package spark.phrase

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.phrase.phraser.{Phrases, PhrasesConfig, Util, Vocab}
import spark.phrase.scorer.BigramScorer

case class CorpusHolder(var min_reduce: Int, vocab: Vocab, var total_words: Int) {

  def merge(that: CorpusHolder): Unit = {
    val (min_reduce, vocab, total_words) = (that.min_reduce, that.vocab, that.total_words)
    this.total_words = this.total_words + that.total_words
    this.min_reduce = Math.max(this.min_reduce, that.min_reduce)
    this.vocab.merge(that.vocab)
    // that.corpus.foreach(x => this.corpus.put(x._1,x._2))
  }
}

object CorpusHolder {

  def learnAndSave(spark: SparkSession, sentencesDf: Dataset[String], configBc: Broadcast[PhrasesConfig], scorer: BigramScorer, outputPath: String = "/tmp/gensim-model"): Unit = {

    import spark.implicits._
    // init phrases - global corpus
    val phrases = Phrases(configBc.value, scorer)

    // add shutdown hook to save global corpus on app shutdown
    sys.ShutdownHookThread {
      println(phrases.pseudoCorpus())
      Util.save(phrases, outputPath)
      spark.stop()
    }

    // collect batch-wise corpus and merge the batch-wise corpus learnt into global corpus i.e., phrases
    // Note: no need to broadcast phrases, as we are collecting corpus array at driver. Broadcast is to sahre a read-only cache with all executors.
    // Also, dont worry about collecting corpus at driver as phrases because mergeSentenceVocabWithCorpus() will limit the retained corpus according to maxVocabSize config param.
    // Just make sure that driver has ample memory to hold maxVocabLen number of words.
    val corpusArray = sentencesDf
      .mapPartitions[CorpusHolder]{ (sentencesInPartitionIter: Iterator[String]) => batchLearn(sentencesInPartitionIter, configBc)}
      .collect
      .foreach(corpus => phrases.mergeSentenceVocabWithCorpus(corpus))
  }

  /**
    * Iterates through each sentence in this batch and collects corpus learnt from it
    */
  def batchLearn(sentenceItr: Iterator[String], configBc: Broadcast[PhrasesConfig]): Iterator[CorpusHolder] = {
    val config = configBc.value
    var batchWiseCorpus: CorpusHolder = null
    var size = 0
    while (sentenceItr.hasNext) {
      size = size + 1
      val sentence = sentenceItr.next()
      val wordsInSentence = Array(sentence.split(" "))
      val corpusLearntFromSentence = Phrases.learnVocab(wordsInSentence, configBc.value)
      if (batchWiseCorpus == null) {
        batchWiseCorpus = corpusLearntFromSentence
      } else {
        batchWiseCorpus.merge(corpusLearntFromSentence)
      }
    }
    Array(batchWiseCorpus).iterator
  }
}