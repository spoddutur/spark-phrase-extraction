# spark-gensim-parser

## Introduction
Gensim is originally a Python library ([found here](https://github.com/RaRe-Technologies/gensim)) for topic modelling, document indexing and similarity retrieval with large corpora. Target audience is the natural language processing (NLP) and information retrieval (IR) community.
**spark-gensim-parser** integrates gensim's collocation phrase detection module with Spark-Scala. It provides:
- Scala interface enabling use of Gensim directly from Scala-Spark.
- DSL to use Spark data structures as input for Gensim's algorithms.
- Basic building blocks to create ML applications utilizing GenSim API's to:
  - To train a distributed corpus vocabulary from Spark Data Structures (RDDs, DataFrames, Datasets)
  - To Save trained model
  - To load a saved model and use that trained model with its corpus knowledge to predict collocated n-gram phrases in input sentences.
  - Scoring:
    - Supports default python-gensim scorers: Original Scoring and NPMI Scoring
    - Enabled config-based-approach to plugin and play custom scorers
    - Added Contingency-Based scorers for use with Phraser like ChiSq, Jaccard, LogLikelyHood etc

## How to Run
- Link to ipynb notebook

## Documentation
- [Official Python-Gensim Documentation](https://radimrehurek.com/gensim/models/phrases.html)
- [Python-Gensim Github](https://github.com/RaRe-Technologies/gensim)

