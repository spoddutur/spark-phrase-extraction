import org.junit.{Before, Test}
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers
import spark.phrase.phraser.Phraser.SENTENCES_TYPE
import spark.phrase.phraser.{Phraser, Phrases, SimplePhrasesConfig}
import spark.phrase.scorer.BigramScorer

import scala.collection.mutable

class TestPhraser extends JUnitSuite with Checkers {

  var sentence_stream: Array[Array[String]] = _
  var expected_bigrams_default_scorer: Seq[Seq[String]] = _
  var expected_trigrams_default_scorer: Seq[Seq[String]] = _
  var expected_bigrams_npmi_scorer: Seq[Seq[String]] = _
  var common_words: mutable.HashSet[String] = _

  @Before
  def init(): Unit = {
    sentence_stream = Array[Array[String]](
      "the mayor of san francisco was there".split(" "),
      "san francisco is beautiful city".split(" "),
      "machine learning coding skills is useful sometimes".split(" "),
      "the city of san francisco is beautiful".split(" "),
      "machine learning coding is done in python".split(" "))

    expected_bigrams_default_scorer = Seq[Seq[String]](
      "the mayor of san_francisco was there".split(" ").toSeq,
      "san_francisco is_beautiful city".split(" ").toSeq,
      "machine_learning coding skills is useful sometimes".split(" ").toSeq,
      "the city of san_francisco is_beautiful".split(" ").toSeq,
      "machine_learning coding is done in python".split(" ").toSeq
    )

    expected_trigrams_default_scorer = Seq[Seq[String]](
      "the mayor of san_francisco was there".split(" ").toSeq,
      "san_francisco_is_beautiful city".split(" ").toSeq,
      "machine_learning_coding skills is useful sometimes".split(" ").toSeq,
      "the city of san_francisco_is_beautiful".split(" ").toSeq,
      "machine_learning_coding is done in python".split(" ").toSeq
    )

    expected_bigrams_npmi_scorer = Seq[Seq[String]](
      Seq[String]("the", "mayor_of_san", "francisco_was", "there"),
      Seq[String]("san_francisco", "is_beautiful", "city"),
      Seq[String]("machine_learning", "coding_skills", "is_useful", "sometimes"),
      Seq[String]("the", "city", "of", "san_francisco", "is_beautiful"),
      Seq[String]("machine_learning", "coding", "is_done", "in_python")
    )
    common_words= mutable.HashSet[String]("of", "with", "without", "and", "or", "the", "a")
  }

  def sentence_ngrams(config: SimplePhrasesConfig, scorer: BigramScorer, sentence_stream: SENTENCES_TYPE, expected_ngrams: Seq[Seq[String]]): SENTENCES_TYPE = {
    val phrases = new Phrases(config, scorer)
    phrases.addVocab(sentence_stream)
    val bigram_phraser = Phraser(phrases)
    val bigrams = sentence_stream.map(sentence => bigram_phraser(sentence))
    val bigramsAsSeq = bigrams.map(x => x.toSeq).toSeq

    // assert generated ngrams with expected_ngrams
    expected_ngrams.foreach(x => {
      assert(bigramsAsSeq.contains(x))
    })
    bigrams
  }

  @Test
  def test_bigrams_and_trigrams_with_default_scorer(): Unit = {
    val config = new SimplePhrasesConfig().copy(minCount=1, threshold=1.0f, commonWords = Some(common_words))
    val default_scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)
    val sentence_bigrams = sentence_ngrams(config, default_scorer, sentence_stream, expected_bigrams_default_scorer)
    val sentence_trigrams = sentence_ngrams(config, default_scorer, sentence_bigrams, expected_trigrams_default_scorer)
  }

  /**
    * npmi_scorer with threshold=0.5
    */
  @Test
  def test_bigrams_npmi_scorer(): Unit = {
    val config = new SimplePhrasesConfig().copy(minCount=1, threshold=0.5f, commonWords = Some(common_words))
    val scorer = BigramScorer.getScorer(BigramScorer.NPMI)
    sentence_ngrams(config, scorer, sentence_stream, expected_bigrams_npmi_scorer)
  }
}
