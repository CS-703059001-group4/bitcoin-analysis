import os
from graphframes import *
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext


def main():
    sc = configure_spark_context()
    sqlContext = SQLContext(sc)

    v = sqlContext.createDataFrame([
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
    ], ["id", "name", "age"])

    e = sqlContext.createDataFrame([
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
    ], ["src", "dst", "relationship"])

    g = GraphFrame(v, e)

    results = g.pageRank(resetProbability=0.01, maxIter=20)
    results.vertices.select("id", "pagerank").show()


def configure_spark_context():
    conf = SparkConf().setAppName("APP_NAME").setMaster(os.getenv("SPARK_MASTER"))
    return SparkContext(conf=conf)


if __name__ == "__main__":
    main()
