
# Spark Phrase Extraction : Automated phrase mining from huge text corpus using Apache Spark

This library is similar to phrase extraction implementation in Gensim([found here](https://github.com/RaRe-Technologies/gensim)) but for huge text corpus at scale using apache Spark. 

[![Build Status](https://travis-ci.org/spoddutur/spark-phrase-extraction.svg?branch=master)](https://travis-ci.org/spoddutur/spark-phrase-extraction)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/spark-phrase-extraction?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=body_badge)
[![codecov](https://codecov.io/gh/spoddutur/spark-phrase-extraction/branch/master/graph/badge.svg)](https://codecov.io/gh/spoddutur/spark-phrase-extraction)

**Target audience:** Spark-Scala ML applications in the need of collocations phrase detection for their natural language processing (NLP) and information retrieval (IR) tasks.
<br/>

## spark-parse-extraction provides:
- Basic building blocks to create ML applications utilizing GenSim API's to:
  - To Train a distributed corpus vocabulary which automatically detects common phrases (multiword expressions) from a stream of sentences. Corpus learnt is based on frequently oocuring collocated phrases.
  - To Save the trained model
  - To Load the saved model and use it with its corpus knowledge to predict collocated n-gram phrases in input sentences.
  - Scoring:
    - Supports default python-gensim scorers: Original Scoring and NPMI Scoring
    - Enabled config-based-approach to plugin and play custom scorers
    - Added Contingency-Based scorers for use with Phraser like ChiSq, Jaccard, LogLikelyHood etc

## How to Run
### Start Training
```markdown
    // init config
    val common_words = mutable.HashSet[String]("of", "with", "without", "and", "or", "the", "a")
    val config: PhrasesConfig = new SimplePhrasesConfig()
                    .copy(minCount = 1, 
                          threshold = 1.0f, 
                          commonWords = Some(common_words))
    val configBc = spark.sparkContext.broadcast(config)
    
    // init scorer
    val scorer = BigramScorer.getScorer(BigramScorer.DEFAULT)

    // read sentences and start learning phrases
    val sentencesDf = spark.read...
    CorpusHolder.learnAndSave(spark, sentencesDf, configBc, scorer, outputPath)
```
- Config params supported are documented [here](https://github.com/spoddutur/spark-phrase-extraction/edit/master/src/main/scala/spark/phrase/phraser/PhrasesConfig.scala)
- `CorpusHolder.learnAndSave` does take in config and scorer, applies them on input sentences and saves the model at outputPath.

### Start Predicting
Prediction involves 2 steps: loading model and use it to predict. 
```markdown
    // load model from output path in step3 above
    val phrases = Util.load[Phrases]("/tmp/gensim-model")
    val phraserBc = spark.sparkContext.broadcast(new Phraser(phrases))
    
    // read input sentences as dataframe
    val sentencesDf = spark.read....
    
    // predict
    val sentenceBigramsDf = sentencesDf
                  .map(sentence => phraserBc.value.apply(sentence.split(" ")))

    // write bigrams to file
    sentenceBigramsDf.write.json("/tmp/gensim-output/")
 ```
 
## Working Examples:
Please refer to [Predictor implementation](https://github.com/spoddutur/spark-phrase-extraction/blob/master/src/main/scala/SparkPhrasePredictor.scala) and [Trainer implementation](https://github.com/spoddutur/spark-phrase-extraction/blob/master/src/main/scala/ClusterPhraseExtractionTrainer.scala) where I've demoed a working example to train and predict phrases.
 
## How to train for Trigrams?
- Above step trains bigrams.
- If we want trigrams, repeat step3 for `sentenceBigramsDf` instead of `sentencesDf`as shown below
```markdown
  CorpusHolder.learnAndSave(spark, sentenceBigramsDf, configBc, scorer, outputPath)
```
- And so on for each of the higher n-grams we want.

## References
- Heavily inspired from the good work that Python-Gensim has done [here](https://radimrehurek.com/gensim/models/phrases.html) and [here](http://pydoc.net/gensim/3.2.0/gensim.models.phrases/)
- [Python-Gensim Github](https://github.com/RaRe-Technologies/gensim)
- [Python-Gensim.ipynb on How to use it](https://github.com/jdwittenauer/ipython-notebooks/blob/master/notebooks/libraries/Gensim.ipynb)
- [Custom Scoring support via contingency-based scoring for collocations and statistical analysis of n-grams](http://dspace.uib.no/bitstream/handle/1956/11033/lyse-andersen-mwe-final.pdf?sequence=1&isAllowed=y)
