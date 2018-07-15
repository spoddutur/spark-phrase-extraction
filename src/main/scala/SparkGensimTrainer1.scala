import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import spark.gensim.{CorpusHolder, SentenceCorpus}
import spark.gensim.phraser.{Phrases, PhrasesConfig, SimplePhrasesConfig, Util}
import spark.gensim.scorer.BigramScorer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

//// http://dspace.uib.no/bitstream/handle/1956/11033/lyse-andersen-mwe-final.pdf?sequence=1&isAllowed=y
object SparkGensimTrainer1 {

  def main(args: Array[String]): Unit = {
    println("main")

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("streaming-gensim-phraser")
      .getOrCreate()

    val common_words= mutable.HashSet[String]("of", "with", "without", "and", "or", "the", "a")
    val config: PhrasesConfig = new SimplePhrasesConfig().copy(minCount=1, threshold=1.0f, commonWords = Some(common_words))
    val configBc = spark.sparkContext.broadcast(config)
    val scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)
    val phrases = Phrases(config, scorer)

    import spark.implicits._
//    val sentencesDf = spark.read
//      .format("text")
//      .load("/tmp/gensim-input").as[String]

    val df0 = spark.read.format("text").load("/Users/surthi/Downloads/parsed_business_articles_total.txt").as[String]
    // s3://newsplatform-config-dev/parsed_business_articles_total.txt
    val sentencesDf = df0.flatMap(content => content.split(" . ").map(x => x.replaceAll("[^a-zA-Z ]", "").trim())).repartition(1000)

    // for each partition block of input data
     val s = sentencesDf.mapPartitions[SentenceCorpus]{(sentencesInPartitionIter: Iterator[String]) => {
       val config = configBc.value
       var output: SentenceCorpus = null
       var size = 0
       while (sentencesInPartitionIter.hasNext) {
         size = size + 1
         val sentence = sentencesInPartitionIter.next()
         val words = Array(sentence.split(" "))
         val sentenceCorpus = Phrases.learnVocab(words, configBc.value)
         if(output == null) {
           output = sentenceCorpus
         } else {
           output.merge(sentenceCorpus)
         }
       }
       println("$$ num partitions: " + size)
       output.corpus
       Array(output).iterator
     }}.collect

    for(scorpus <- s) {
      phrases.mergeSentenceVocabWithCorpus(scorpus)
    }

    sys.ShutdownHookThread {
      println(phrases.pseudoCorpus())
      Util.save(phrases, "/tmp/gensim-model")
      println("***************************************")
      spark.stop()
    }
  }

  def withBroadcast[T: ClassTag, Q](v: T)(f: Broadcast[T] => Q)(implicit sc: SparkContext): Q = {
    val broadcast = sc.broadcast(v)
    val result = f(broadcast)
    broadcast.unpersist()
    result
  }
}
