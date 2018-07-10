# spark-gensim-parser

Gensim is originally a Python library ([found here](https://github.com/RaRe-Technologies/gensim)) for topic modelling, document indexing and similarity retrieval with large corpora. [**spark-gensim-parser**](https://github.com/spoddutur/spark-gensim-parser) integrates [gensim's collocation phrase detection python module](https://github.com/RaRe-Technologies/gensim/blob/develop/gensim/models/phrases.py) with Spark-Scala.

<br/>

**Integrated Version:** <img src="https://user-images.githubusercontent.com/22542670/42492038-13f9f8ec-8435-11e8-830e-9d7152acb421.png" width="200"/>

**Target audience:** is the natural language processing (NLP) and information retrieval (IR) community.
<br/>

## spark-gensim-parser provides:

- Scala interface enabling use of Gensim directly from Scala-Spark.
- DSL to use Spark data structures as input for Gensim's algorithms.
- Basic building blocks to create ML applications utilizing GenSim API's to:
  - To Train a distributed corpus vocabulary
  - To Save the trained model
  - To load a saved model and use it with its corpus knowledge to predict collocated n-gram phrases in input sentences.
  - Scoring:
    - Supports default python-gensim scorers: Original Scoring and NPMI Scoring
    - Enabled config-based-approach to plugin and play custom scorers
    - Added Contingency-Based scorers for use with Phraser like ChiSq, Jaccard, LogLikelyHood etc

## How to Run
- Link to ipynb notebook

## References
- [Official Python-Gensim Documentation](https://radimrehurek.com/gensim/models/phrases.html)
- [Python-Gensim Github](https://github.com/RaRe-Technologies/gensim)
- [Python-Gensim.ipynb on How to use it](https://github.com/jdwittenauer/ipython-notebooks/blob/master/notebooks/libraries/Gensim.ipynb)
- [Custom Scoring support via contingency-based scoring for collocations and statistical analysis of n-grams](http://dspace.uib.no/bitstream/handle/1956/11033/lyse-andersen-mwe-final.pdf?sequence=1&isAllowed=y)

