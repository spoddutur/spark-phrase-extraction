package spark.gensim

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import spark.gensim.phraser.{Phrases, PhrasesConfig, Util, Vocab}
import spark.gensim.scorer.BigramScorer

import scala.collection.mutable

case class SentenceCorpus(var min_reduce: Int, corpus: mutable.HashMap[String, Int], var total_words: Int) {

  def merge(that: SentenceCorpus): Unit = {
    val (min_reduce, vocab, total_words) = (that.min_reduce, that.corpus, that.total_words)
    this.total_words = this.total_words + that.total_words
    this.min_reduce = Math.max(this.min_reduce, that.min_reduce)
    that.corpus.foreach(x => this.corpus.put(x._1,x._2))
  }
}

object SentenceCorpus {

  def merge(sentenceVocabs: Array[SentenceCorpus], config: PhrasesConfig): SentenceCorpus = {
    var corpus_word_count = 0
    var corpus_min_reduce = 0
    var corpus_vocab: Vocab = null
    for(sentenceVocab <- sentenceVocabs) {
      val (sentence_min_reduce, sentence_corpus, sentence_total_words) = (sentenceVocab.min_reduce, sentenceVocab.corpus, sentenceVocab.total_words)
      corpus_word_count = corpus_word_count + sentence_total_words

      if (corpus_vocab != null && !corpus_vocab.isEmpty()) {

        corpus_min_reduce = Math.max(corpus_min_reduce, sentence_min_reduce)
        corpus_vocab.merge(sentence_corpus)
        if (corpus_vocab.size() > config.maxVocabSize) {
          corpus_vocab.pruneVocab(corpus_min_reduce)
          corpus_min_reduce = corpus_min_reduce + 1
        }
      } else {
        corpus_vocab = new Vocab(config.delimiter, sentence_corpus)
      }
    }
    SentenceCorpus(corpus_min_reduce, corpus_vocab.wordCounts, corpus_word_count)
  }
}
object CorpusHolder {

  var phrasesBc: Broadcast[Phrases] = _

  def init(spark: SparkSession, config: PhrasesConfig, scorer: BigramScorer): Unit = {
    val phrases = Phrases(config, scorer)
    phrasesBc = spark.sparkContext.broadcast(phrases)
    spark.sparkContext.addSparkListener(new SparkMonitoringListener(spark, phrasesBc))
  }

  def update1(spark: SparkSession, sentences: Dataset[String], configBc: Broadcast[PhrasesConfig]): Unit = {
    import spark.implicits._
    val sentenceCorpusDs = sentences.foreach(sentence => {
      val a = Array(sentence.split(" "))
      val sv = Phrases.learnVocab(a, configBc.value)
      CorpusHolder.phrasesBc.value.mergeSentenceVocabWithCorpus(sv)
    })

    // sentenceCorpusDs.persist(StorageLevel.MEMORY_AND_DISK)
    //    phrasesBc.unpersist(true)
    //    phrasesBc = spark.sparkContext.broadcast(null)
    //    sentencesDf.foreach(sentence => phrasesBc.value.addVocab(Seq(sentence.split(" ")).toArray))
  }

  def update(spark: SparkSession, sentences: Dataset[String], configBc: Broadcast[PhrasesConfig]): Unit = {
    import spark.implicits._
    val sentenceCorpusDs = sentences.map(sentence => {
      val a = Array(sentence.split(" "))
      Phrases.learnVocab(a, configBc.value)
    })
    sentenceCorpusDs.createGlobalTempView("sentence_corpus")
    sentenceCorpusDs.show()

    // sentenceCorpusDs.persist(StorageLevel.MEMORY_AND_DISK)
//    phrasesBc.unpersist(true)
//    phrasesBc = spark.sparkContext.broadcast(null)
//    sentencesDf.foreach(sentence => phrasesBc.value.addVocab(Seq(sentence.split(" ")).toArray))
  }
}

case class SparkMonitoringListener(spark: SparkSession, var phrasesBc: Broadcast[Phrases]) extends SparkListener {

  // update corpus
  override def onTaskEnd(jobEnd: SparkListenerTaskEnd): Unit = {
    import spark.implicits._
    val x = spark.sql("SELECT * FROM global_temp.sentence_corpus").as[SentenceCorpus].collectAsList()
    val phrases = phrasesBc.value
//    x.foreach(r => {
//      println(x)
//      phrases.mergeSentenceVocabWithCorpus(r)
//    })
    val size = x.size()
    for(i <- 0 to size-1) {
      phrases.mergeSentenceVocabWithCorpus(x.get(i))
    }
    phrasesBc.unpersist(true)
    spark.sql("select * from global_temp.sentence_corpus").show()
    spark.sql("TRUNCATE table global_temp.sentence_corpus")
    spark.sql("select * from global_temp.sentence_corpus").show()
    phrasesBc = spark.sparkContext.broadcast(phrases)
  }

  // save corpus
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    Util.save(phrasesBc.value, "/tmp/gensim-model1")
    super.onApplicationEnd(applicationEnd)
  }
}