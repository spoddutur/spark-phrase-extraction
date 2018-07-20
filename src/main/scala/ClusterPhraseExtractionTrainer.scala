import org.apache.spark.sql.SparkSession
import spark.phrase.CorpusHolder
import spark.phrase.phraser.{PhrasesConfig, SimplePhrasesConfig}
import spark.phrase.scorer.BigramScorer

import scala.collection.mutable

object ClusterPhraseExtractionTrainer {

  def main(args: Array[String]): Unit = {
    println("main")

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("streaming-gensim-phraser")
      .getOrCreate()

    // init config with params like common_words, mincount, threshold etc
    val common_words = mutable.HashSet[String]("of", "with", "without", "and", "or", "the", "a")
    val config: PhrasesConfig = new SimplePhrasesConfig().copy(minCount = 1, threshold = 1.0f, commonWords = Some(common_words))
    val configBc = spark.sparkContext.broadcast(config)

    // fetch any of the bigram scorers: DEFAULT, NPMI, JACCARD, CHI_SQ
    val scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)

    // read input sentences dataframes
    import spark.implicits._
    val df0 = spark.read.format("text").load("/Users/surthi/Downloads/parsed_business_articles_total.txt").as[String]
    val sentencesDf = df0.flatMap(content => content.split(" . ").map(x => x.replaceAll("[^a-zA-Z ]", "").trim())).repartition(1000)

    // Start learning the corpus. It'll be saved at /tmp/gensim-model location on finish.
    CorpusHolder.learnAndSave(spark, sentencesDf, configBc, scorer, "/tmp/gensim-model")
  }
}
