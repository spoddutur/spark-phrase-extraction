import org.scalatest._
import org.scalatest.junit.JUnitSuite
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._
import org.scalatest.prop.Checkers
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import spark.gensim.phraser._
import spark.gensim.scorer.{BigramScorer, DefaultBigramScorer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BaseTestPhrases extends JUnitSuite with Checkers {
  var p: Phrases = _

  @Before
  def init(): Unit = {
    p = new Phrases(new SimplePhrasesConfig().copy(minCount = 1, threshold = 1), DefaultBigramScorer)
  }
}

class TestPhrases extends BaseTestPhrases {

  @Test
  def test_pseudocorpus_no_common_terms() {
    p.corpus_vocab = new Vocab(p.config.delimiter)
    p.corpus_vocab.addWord("prime_minister", 2)
    p.corpus_vocab.addWord("gold", 2)
    p.corpus_vocab.addWord("chief_technical_officer", 2)
    p.corpus_vocab.addWord("effective", 2)

    val pc = p.pseudoCorpus().map(x => x.toSeq).toSeq
    assert(pc.size.equals(3))
    assert(pc.contains(Seq[String]("prime", "minister")))
    assert(pc.contains(Seq[String]("chief", "technical_officer")))
    assert(pc.contains(Seq[String]("chief_technical", "officer")))
  }

  @Test
  def test_pseudocorpus_with_common_terms() {
    p.corpus_vocab = new Vocab(p.config.delimiter)
    p.corpus_vocab.addWord("hall_of_fame", 2)
    p.corpus_vocab.addWord("gold", 2)
    p.corpus_vocab.addWord("chief_of_political_bureau", 2)
    p.corpus_vocab.addWord("effective", 2)
    p.corpus_vocab.addWord("beware_of_the_dog_in_the_yard", 2)

    p.config.commonWords = Some(mutable.HashSet[String]("in", "the", "of"))
    val pc = p.pseudoCorpus().map(x => x.toSeq).toSeq
    assert(pc.size.equals(5))
    assert(pc.contains(Seq("beware", "of", "the", "dog_in_the_yard")))
    assert(pc.contains(Seq("beware_of_the_dog", "in", "the", "yard")))
    assert(pc.contains(Seq("chief", "of", "political_bureau")))
    assert(pc.contains(Seq("chief_of_political", "bureau")))
    assert(pc.contains(Seq("hall", "of", "fame")))
  }
}

class TestPhrasesData extends BaseTestPhrases {

  def exportPhrasesWithScore(phrasesModel: Phrases, corpusSentences: Phraser.SENTENCES_TYPE, sentencesToTest: Phraser.SENTENCES_TYPE): mutable.HashMap[Array[String], Double] = {

    phrasesModel.corpus_vocab.clear()
    phrasesModel.addVocab(corpusSentences)
    phrasesModel.exportPhrasesAsTuples(sentencesToTest)
  }

  def exportPhrases(phrasesModel: Phrases,  corpusSentences: Phraser.SENTENCES_TYPE, sentencesToTest: Phraser.SENTENCES_TYPE): Set[String] = {
    exportPhrasesWithScore(phrasesModel, corpusSentences, sentencesToTest).map(x => x._1.mkString(" ")).toSet
  }

  // @Test
  def testExportPhrases(): Unit = {
    // """Test Phrases bigram export_phrases functionality."""

    val bigrams = exportPhrases(p, PhrasesData.sentences, PhrasesData.sentences)

    // expected bigrams "response time", "graph minors", "human interface"
    assert(bigrams.size.equals(3))
    assert(bigrams.contains("response time"))
    assert(bigrams.contains("graph minors"))
    assert(bigrams.contains("human interface"))

  }

  @Test
  def testMultipleBigramsSingleEntry(): Unit = {
    val sentences = Array[Array[String]]("graph minors survey human interface".split(" "))
    val bigrams = exportPhrases(p,PhrasesData.sentences, sentences)
    assert(bigrams.diff(Set[String]("graph minors", "human interface")).isEmpty)
    assert(bigrams.size.equals(2))
    assert(bigrams.contains("graph minors"))
    assert(bigrams.contains("human interface"))
  }

  @Test
  def testScoringDefault(): Unit = {
    val phrasesWithScores = exportPhrasesWithScore(p, PhrasesData.sentences, Array[Array[String]]("graph minors survey human interface".split(" ")))
    val phraseToScore = new mutable.HashMap[String, Double]()
    val scores = ListBuffer[Double]()
    for((phrases, score) <- phrasesWithScores) {
      val bigram = phrases.mkString(" ")
      scores += Math.round(score * 1000)/1000.toDouble
    }
    assert(scores.size.equals(2))
    assert(scores.contains(5.167))
    assert(scores.contains(3.444))
  }

  @Test
  def testScoringNpmi(): Unit = {
    val testSentences = Array[Array[String]]("graph minors survey human interface".split(" "))
    val config = new SimplePhrasesConfig().copy(threshold=0.5f, minCount=1)
    val npmi_p = new Phrases(config, BigramScorer.getScorer(BigramScorer.NPMI))

    val phrasesWithScores = exportPhrasesWithScore(npmi_p, PhrasesData.sentences, testSentences)
    val scores = ListBuffer[Double]()
    for((phrases, score) <- phrasesWithScores) {
      val bigram = phrases.mkString(" ")
      scores += Math.round(score * 1000)/1000.toDouble
    }
    assert(scores.size.equals(2))
    assert(scores.contains(0.882))
    assert(scores.contains(0.714))
    assert(scores.diff(Seq[Double](0.882, 0.714)).isEmpty)
  }

 @Test
  def testPersistence(): Unit = {
    val phrases = new Phrases(new SimplePhrasesConfig().copy(threshold=1, minCount=0), BigramScorer.getScorer(BigramScorer.NPMI))
    phrases.addVocab(PhrasesData.sentences)
    Util.save[Phrases](phrases, "/tmp/phrases")
    val loaded_phrases = Util.load[Phrases]("/tmp/phrases")
    assert(phrases.config.equals(loaded_phrases.config))
    assert(phrases.corpus_vocab.equals(loaded_phrases.corpus_vocab))
    assert(phrases.corpus_word_count.equals(loaded_phrases.corpus_word_count))
    assert(phrases.scorer.equals(loaded_phrases.scorer))
 }

  @Test
  def testPruning(): Unit = {
    val phrases = new Phrases(new SimplePhrasesConfig().copy(maxVocabSize = 5), DefaultBigramScorer)
    val bigrams = exportPhrases(phrases, PhrasesData.sentences, PhrasesData.sentences)
    assertTrue(bigrams.size <= 5)
  }
}

object PhrasesData {
  val sentences = Array[Array[String]](
  Array[String]("human", "interface", "computer"),
    Array[String]("survey", "user", "computer", "system", "response", "time"),
      Array[String]("eps", "user", "interface", "system"),
        Array[String]("system", "human", "system", "eps"),
          Array[String]("user", "response", "time"),
            Array[String]("trees"),
              Array[String]("graph", "trees"),
                Array[String]("graph", "minors", "trees"),
                  Array[String]("graph", "minors", "survey"),
                    Array[String]("graph", "minors", "survey", "human", "interface"))

  val common_terms = Array[String]()

  val bigram1 = "response_time"
  val bigram2 = "graph_minors"
  val bigram3 = "human_interface"

}
trait CommonTermsPhrasesData {
 // """This mixin permits to reuse the test, using, this time the common_terms option """

  val sentences = Array[String](
    "human interface with computer",
    "survey of user computer system lack of interest",
    "eps user interface system",
    "system and human system eps",
    "user lack of interest",
    "trees",
    "graph of trees",
    "data and graph of trees",
    "data and graph of survey",
    "data and graph survey for human interface" // test bigrams within same sentence
  )

  val common_terms = Array[String]("of", "and", "for")
  val bigram1 = "lack_of_interest"
  val bigram2 = "data_and_graph"
  val bigram3 = "human_interface"
  val expression1 = "lack of interest"
  val expression2 = "data and graph"
  val expression3 = "human interface"
}
