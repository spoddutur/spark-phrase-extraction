
# Spark Phrase Extraction : Automated phrase mining from huge text corpus using Apache Spark

This library is similar to phrase extraction implementation in Gensim([found here](https://github.com/RaRe-Technologies/gensim)) but for huge text corpus at scale using apache Spark. 


[![Build Status](https://travis-ci.org/spoddutur/spark-gensim-parser.svg?branch=master)](https://travis-ci.org/spoddutur/spark-gensim-parser)

**Target audience:** Spark-Scala ML applications in the need of collocations phrase detection for their natural language processing (NLP) and information retrieval (IR) tasks.
<br/>

## spark-gensim-parser provides:

- Scala interface enabling use of Gensim directly from Scala-Spark.
- DSL to use Spark data structures as input for Gensim's algorithms.

## How to Run
- Link to ipynb notebook (TODO)

## References
- [Official Python-Gensim Documentation](https://radimrehurek.com/gensim/models/phrases.html)
- [Python-Gensim Github](https://github.com/RaRe-Technologies/gensim)
- [Python-Gensim.ipynb on How to use it](https://github.com/jdwittenauer/ipython-notebooks/blob/master/notebooks/libraries/Gensim.ipynb)
- [Custom Scoring support via contingency-based scoring for collocations and statistical analysis of n-grams](http://dspace.uib.no/bitstream/handle/1956/11033/lyse-andersen-mwe-final.pdf?sequence=1&isAllowed=y)

## Appendix
- Basic building blocks to create ML applications utilizing GenSim API's to:
  - To Train a distributed corpus vocabulary
  - To Save the trained model
  - To load a saved model and use it with its corpus knowledge to predict collocated n-gram phrases in input sentences.
  - Scoring:
    - Supports default python-gensim scorers: Original Scoring and NPMI Scoring
    - Enabled config-based-approach to plugin and play custom scorers
    - Added Contingency-Based scorers for use with Phraser like ChiSq, Jaccard, LogLikelyHood etc
