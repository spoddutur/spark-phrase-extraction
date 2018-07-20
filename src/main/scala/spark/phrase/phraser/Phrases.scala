package spark.phrase.phraser

import spark.phrase.CorpusHolder
import spark.phrase.scorer.BigramScorer

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

case class Phrases(config: PhrasesConfig, scorer: BigramScorer) extends Serializable {

  var corpus_word_count = 0
  var corpus_min_reduce = 0
  var corpus_vocab = new Vocab(config.delimiter)
  val out_delimiter = " "

  def addVocab(sentences: Phraser.SENTENCES_TYPE): Unit = {
    val sentenceCorpus = Phrases.learnVocab(sentences, this.config)
    mergeSentenceVocabWithCorpus(sentenceCorpus)
  }

  def mergeSentenceVocabWithCorpus(sentenceVocab: CorpusHolder): Unit = {
    val (min_reduce, vocab, total_words) = (sentenceVocab.min_reduce, sentenceVocab.vocab, sentenceVocab.total_words)
    corpus_word_count = corpus_word_count + total_words

    if (!corpus_vocab.isEmpty()) {
      println("merging %d counts into %d".format(vocab.size, corpus_vocab.size()))

      this.corpus_min_reduce = Math.max(this.corpus_min_reduce, min_reduce)
      corpus_vocab.merge(vocab)
      if (corpus_vocab.size() > config.maxVocabSize) {
        corpus_vocab.pruneVocab(this.corpus_min_reduce)
        corpus_min_reduce = corpus_min_reduce + 1
      }
      println("merged %d counts into %d".format(vocab.size, corpus_vocab.size()))
    } else {
      corpus_vocab = vocab
    }
  }

  def exportPhrasesAsTuples(sentences: Phraser.SENTENCES_TYPE): mutable.HashMap[Array[String], Double] = {

    var bigramsWithScores = exportPhrases(sentences)
    return bigramsWithScores.map(x => (x._1.split(config.delimiter), x._2))
  }

    /**
    * Get all bigram-phrases that appear in input 'sentences' that pass the bigram threshold.
    *
    * @param sentences
    * @return a HashMap[bigram_phrase, phrase_score]
    */
  def exportPhrases(sentences: Phraser.SENTENCES_TYPE): mutable.HashMap[String, Double] = {

    val out = new mutable.HashMap[String, Double]()
    for (sentence <- sentences) {
      val phraseScores = DefaultSentenceAnalyzer.analyze(sentence, this, this.scorer) // TODO: pass scorer

      // keeps only not None scores
      val filteredPhraseScores = phraseScores.filter(x => x._2.isDefined)
      for ((phrase, score) <- filteredPhraseScores) {
        out.put(phrase, score.get)
      }
    }
    return out.map(x => (x._1, x._2))
  }

  /**
    * Feeds `source_vocab`'s compound keys back to it, to discover phrases.
    * Example: corpus of "prime minister" and "chief technical officer" will discover compund keys such as
    * Array[Array("prime", "minister"), Array("chief_technical", "officer"), Array("chief", "technical_officer")].
    * @return
    */
  def pseudoCorpus(): Array[Array[String]] = {

    var out: ListBuffer[Array[String]] = new ListBuffer[Array[String]]()
    for ((word, count) <- corpus_vocab.wordCounts) {

      val is_unigram = !word.contains(config.delimiter)
      if (!is_unigram) {
        val tokens = word.split(config.delimiter)
        for (i <- 1 to tokens.length-1) {
          val ithOutput = new ListBuffer[String]()
          val token = tokens(i - 1)
          if (!config.isCommonWord(token)) {

            var chain_of_common_terms = new ListBuffer[String]()
            breakable {
              for (j <- i to tokens.length-1) {
                if (config.isCommonWord(tokens(j))) {
                  chain_of_common_terms += tokens(j)
                } else {
                  break
                }
              }
            }

            // dont join common_terms
            ithOutput += tokens.slice(0, i).mkString(config.delimiter)
            ithOutput ++= chain_of_common_terms
            if (i + chain_of_common_terms.size < tokens.length) {
              val tail = tokens.slice(i + chain_of_common_terms.size, tokens.length)
              ithOutput += tail.mkString(config.delimiter)
            }
          }
          if(!ithOutput.isEmpty) {
            out += ithOutput.toArray
          }
        }
      }
    }
    out.toArray
  }
}

object Phrases {

  def learnVocab(sentences: Phraser.SENTENCES_TYPE, config: PhrasesConfig): CorpusHolder = {

    // """Collect unigram/bigram counts from the `sentences` iterable."""

    var sentence_no = -1
    var total_words = 0

    // println("collecting all words and their counts:" + sentences)
    var vocab_for_given_sentences = new Vocab(config.delimiter)
    var min_reduce = 1
    for (sentence <- sentences) {
      sentence_no = sentence_no + 1
      if (sentence_no % config.progressPer == 0) {
        // println("PROGRESS: at sentence #%d, processed %d words and %d word types".format(sentence_no, total_words, vocab_for_given_sentences.size))
      }

      // get nGrams for sentence
      var last_uncommon_word: Option[String] = None
      var in_between_words: Option[ListBuffer[String]] = None
      for (word <- sentence) {
        val is_not_common = !config.isCommonWord(word)
        if (is_not_common) {
          vocab_for_given_sentences.addWord(word, 1)
          if (last_uncommon_word.isDefined) {
            var chain = Seq(last_uncommon_word.get)
            if(in_between_words.isDefined) {
              chain = chain ++ in_between_words.get
            }
            chain = chain ++ Seq(word)
            vocab_for_given_sentences.addWord(chain.mkString(vocab_for_given_sentences.delimiter), 1)
          }
          last_uncommon_word = Some(word)
          in_between_words = None
        } else if (last_uncommon_word.isDefined) {
          if(in_between_words.isDefined) {
            in_between_words.get += word
          } else {
            in_between_words = Some(ListBuffer[String](word))
          }
        }
        total_words = total_words + 1
      }

      if (vocab_for_given_sentences.size() > config.maxVocabSize) {
        // prune
        vocab_for_given_sentences.pruneVocab(min_reduce)
        min_reduce = min_reduce + 1
      }
    }
    // println("collected %d word types from a corpus of %d words (unigram + bigrams) and %d sentences".format(vocab_for_given_sentences.size, total_words, sentence_no + 1))
    // println("Vocab for given sentence:" + vocab_for_given_sentences.wordCounts.mkString(","))
    return CorpusHolder(min_reduce, vocab_for_given_sentences, total_words)
  }

  // main() to run phrases on random sentence_stream and check the pseudo_corpus.
  def main(args: Array[String]): Unit = {
    val config = DefaultPhrasesConfig
    val scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)
    val p = new Phrases(DefaultPhrasesConfig, scorer)
    p.corpus_vocab = new Vocab(config.delimiter)
    p.corpus_vocab.addWord("prime_minister", 2)
    p.corpus_vocab.addWord("gold", 2)
    p.corpus_vocab.addWord("chief_technical_officer", 2)
    p.corpus_vocab.addWord("effective", 2)

    assert(p.pseudoCorpus().diff(ListBuffer[Seq[String]](Seq("prime", "minister"), Seq("chief", "technical_officer"), Seq("chief_technical", "officer"))).isEmpty)

    p.corpus_vocab = new Vocab(config.delimiter)
    p.corpus_vocab.addWord("hall_of_fame", 2)
    p.corpus_vocab.addWord("gold", 2)
    p.corpus_vocab.addWord("chief_of_political_bureau", 2)
    p.corpus_vocab.addWord("effective", 2)
    p.corpus_vocab.addWord("beware_of_the_dog_in_the_yard", 2)

    p.config.commonWords = Some(mutable.HashSet[String]("in", "the", "of"))
    println(p.pseudoCorpus())
    assert(p.pseudoCorpus().diff(ListBuffer(Seq("beware", "of", "the", "dog_in_the_yard"), Seq("beware_of_the_dog", "in", "the", "yard"), Seq("chief", "of", "political_bureau"), Seq("chief_of_political", "bureau"), Seq("hall", "of", "fame"))).isEmpty)
  }
}
