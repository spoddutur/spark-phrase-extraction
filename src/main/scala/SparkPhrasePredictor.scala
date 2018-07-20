import org.apache.spark.sql.SparkSession
import spark.phrase.phraser.{Phraser, Phrases, Util}

object SparkPhrasePredictor {

  def main(args: Array[String]): Unit = {
    println("main")

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("streaming-gensim-phraser")
      .getOrCreate()

    // load trained model and broadcast it
    val phrases = Util.load[Phrases]("/tmp/gensim-model")
    val phraserBc = spark.sparkContext.broadcast(new Phraser(phrases))

    // read input
    import spark.implicits._
    val sentencesDf = spark.read
      .format("text")
      .load("/tmp/gensim-input").as[String]

    // start extracting phrases from input sentences using model
    val sentenceBigramsDf = sentencesDf.map(sentence => phraserBc.value.apply(sentence.split(" ")))
    sentenceBigramsDf.write.json("/tmp/gensim-output/")

    sys.ShutdownHookThread {
      spark.stop()
    }
  }
}
