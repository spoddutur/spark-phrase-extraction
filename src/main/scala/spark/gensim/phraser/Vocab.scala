package spark.gensim.phraser

import scala.collection.mutable.HashMap

/**
  *
  * @param delimiter if word is ngram, delimiter is used to concat all n-gram tokens together as one word.
  * @param wordCounts
  */
case class Vocab(delimiter: String, wordCounts: HashMap[String, Int] = new HashMap[String, Int]) {

  def clear(): Unit = {
    this.wordCounts.clear()
  }

  def addWord(word: String, count: Int = 1): Unit = {
    val countOfWordInVocab = this.wordCounts.getOrElse(word, 0)
    this.wordCounts.put(word, countOfWordInVocab + count)
  }

  def size(): Int = this.wordCounts.size

  def isEmpty(): Boolean = this.wordCounts.isEmpty

  def contains(word: String): Boolean = {
    this.wordCounts.contains(word)
  }

  def getCount(word: String): Int = {
    if (contains(word) && this.wordCounts.get(word).isDefined) this.wordCounts.get(word).get else 0
  }

  def getWords(): Seq[String] = {
    this.wordCounts.keys.toSeq
  }

  def merge(vocab: Vocab): Unit = {
    for(e <- vocab.wordCounts) {
      this.addWord(e._1, e._2)
    }
  }

  private def numDelimiters(word: String): Int = {
    0.until(word.length).filter(word.startsWith(delimiter, _)).size
  }

  /**
    * Removes all words with frequency count < minCount in vocab.
    * @param minCount - Frequency threshold for tokens in `vocab`.
    */
  def pruneVocab(minCount: Int): Unit = {

    var wordsSet = this.wordCounts.keySet
    for(word <- wordsSet) {
      if(this.wordCounts.getOrElse(word, 0)  < minCount) {
        this.wordCounts.remove(word)
      }
    }
  }
}
