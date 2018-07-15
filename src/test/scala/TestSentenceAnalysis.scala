import org.junit.Test
import org.junit.Before
import spark.phrase.phraser._
import spark.phrase.scorer.{BigramScorer, DefaultBigramScorer}

import scala.collection.mutable

object TestAnalyzer extends SentenceAnalyzer {

  var scores: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()

  def init(scores: mutable.HashMap[String, Double]): Unit = {
    this.scores = scores
  }

  override def scoreItem(worda: String, wordb: String, components: Seq[String], phrases_model: Phrases, scorer: BigramScorer): Double = {
    if (worda != null && wordb != null) {
      val bigram = components.mkString(phrases_model.config.delimiter)
      return this.scores.getOrElse(bigram, -1)
    } else {
      return -1
    }
  }
}

object TestSentenceAnalysis {
  val commonWords = Some(mutable.HashSet[String]("a", "the", "with", "of"))
  val COMMON_WORDS_CONFIG = new SimplePhrasesConfig().copy(threshold = 1, minCount=1, commonWords = commonWords)
  val NO_COMMON_WORDS_CONFIG = new SimplePhrasesConfig().copy(threshold = 1, minCount=1, commonWords = None)

  val COMMON_WORDS_PHRASES = new Phrases(TestSentenceAnalysis.COMMON_WORDS_CONFIG, DefaultBigramScorer)
  val NO_COMMON_WORDS_PHRASES = new Phrases(TestSentenceAnalysis.NO_COMMON_WORDS_CONFIG, DefaultBigramScorer)
}
class TestSentenceAnalysis {

  var p: Phrases = _
  @Before
  def init(): Unit = {
    p = TestSentenceAnalysis.COMMON_WORDS_PHRASES
  }

  def analyze(sentence: String): scala.collection.Set[String] = {
    TestAnalyzer.analyze(sentence.split(" "), p, p.scorer).map(x => x._1).toSet
  }

  @Test
  def test_simple_analysis(): Unit = {

    assert(mutable.HashSet[String]("simple", "sentence", "should", "pass").diff(analyze("simple sentence should pass")).isEmpty)
    assert(mutable.HashSet[String]("simple", "sentence", "with", "no", "bigrams", "but", "common", "terms").diff(analyze("simple sentence with no bigrams but common terms")).isEmpty)
  }

  @Test
  def test_analysis_bigrams(): Unit = {
    // p.corpus_vocab.clear()

    TestAnalyzer.init(mutable.HashMap[String, Double](
      ("simple_sentence", 2),
      ("sentence_many", 2),
      ("many_possible", 2),
      ("possible_bigrams", 2)
    ))

    var bigrams = analyze("simple sentence many possible bigrams")
    var expected = mutable.HashSet[String]("simple_sentence", "many_possible", "bigrams")
    assert(expected.diff(bigrams).isEmpty)

    bigrams = analyze("some simple sentence many bigrams")
    expected = mutable.HashSet[String]("some", "simple_sentence", "many", "bigrams")
    assert(expected.diff(bigrams).isEmpty)

    bigrams = analyze("some unrelated simple words")
    expected = mutable.HashSet[String]("some", "unrelated", "simple", "words")
    assert(expected.diff(bigrams).isEmpty)
  }

  @Test
  def test_analysis_common_terms(): Unit = {
    TestAnalyzer.init(mutable.HashMap[String, Double](
      ("simple_sentence", 2),
      ("sentence_many", 2),
      ("many_possible", 2),
      ("possible_bigrams", 2)
    ))

    var bigrams = analyze("a simple sentence many the possible bigrams")
    var expected = mutable.HashSet[String]("a", "simple_sentence", "many", "the", "possible_bigrams")
    assert(expected.diff(bigrams).isEmpty)

    bigrams = analyze("simple the sentence and many possible bigrams with a")
    expected = mutable.HashSet[String]("simple", "the", "sentence", "and", "many_possible", "bigrams", "with", "a")
    assert(expected.diff(bigrams).isEmpty)
  }

  @Test
  def test_analysis_common_terms_in_between(): Unit = {
    TestAnalyzer.init(mutable.HashMap[String, Double](
      ("simple_sentence", 2),
      ("sentence_with_many", 2),
      ("many_possible", 2),
      ("many_of_the_possible", 2),
      ("possible_bigrams", 2)
    ))

    var bigrams = analyze("sentence with many possible bigrams")
    var expected = mutable.HashSet[String]("sentence_with_many", "possible_bigrams")
    assert(expected.diff(bigrams).isEmpty)

    bigrams = analyze("a simple sentence many of the possible bigrams")
    expected = mutable.HashSet[String]("a", "simple_sentence", "many_of_the_possible", "bigrams")
    println(bigrams)
    assert(expected.diff(bigrams).isEmpty)
  }
}
