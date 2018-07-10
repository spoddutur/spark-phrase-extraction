import org.apache.spark.sql.SparkSession
import spark.gensim.phraser.{Phrases, SimplePhrasesConfig, Util}
import spark.gensim.scorer.BigramScorer

import scala.collection.mutable

//// http://dspace.uib.no/bitstream/handle/1956/11033/lyse-andersen-mwe-final.pdf?sequence=1&isAllowed=y
object SparkGensimTrainer {

  def main(args: Array[String]): Unit = {
    println("main")

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("streaming-gensim-phraser")
      .getOrCreate()

    val common_words= mutable.HashSet[String]("of", "with", "without", "and", "or", "the", "a")
    val config = new SimplePhrasesConfig().copy(minCount=1, threshold=1.0f, commonWords = Some(common_words))
    val scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)
    val phrases = Phrases(config, scorer)
    val phrasesBc = spark.sparkContext.broadcast(phrases)

    import spark.implicits._
    val sentencesDf = spark.read
      .format("text")
      .load("/tmp/gensim-input").as[String]

    val df = spark.sparkContext.parallelize(Array[String]("")).toDF()

    sentencesDf.foreach(sentence => phrasesBc.value.addVocab(Seq(sentence.split(" ")).toArray))

    sys.ShutdownHookThread {
      println(phrases.pseudoCorpus())
      Util.save(phrases, "/tmp/gensim-model")
      spark.stop()
    }
  }
}
