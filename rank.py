from datetime import datetime
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
bucket = spark._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = spark._jsc.hadoopConfiguration().get('fs.gs.project.id')
bq_dataset = 'bitcoin'
bq_vertices_table = 'tx_2018_vertices_data'
bq_edges_table = 'tx_2018_edges_data'

from graphframes import GraphFrame


def main():
    print('Read data from BigQuery')
    vertices = load_data(bq_vertices_table)
    edges = load_data(bq_edges_table)
    graph = GraphFrame(vertices, edges)
    print('Find the largest connected subgraph')
    subgraph = find_the_largest_subgraph(graph)
    print('Caculate pagerank')
    results = subgraph.pageRank(resetProbability=0.15, maxIter=10)
    results.vertices\
        .select('id', 'pagerank')\
        .orderBy(desc('pagerank'))\
        .limit(10)\
        .show()
    spark.stop()


def load_data(table):
    todays_date = datetime.strftime(datetime.today(), '%Y-%m-%d-%H-%M-%S')
    input_directory = 'gs://{}/tmp/bitcoin-{}-{}'.format(table, bucket, todays_date)
    conf = {
        'mapred.bq.project.id': project,
        'mapred.bq.gcs.bucket': bucket,
        'mapred.bq.temp.gcs.path': input_directory,
        'mapred.bq.input.project.id': project,
        'mapred.bq.input.dataset.id': bq_dataset,
        'mapred.bq.input.table.id': table,
    }
    table_data = spark.sparkContext.newAPIHadoopRDD(
        'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'com.google.gson.JsonObject',
        conf=conf)
    table_json = table_data.map(lambda x: x[1])
    return spark.read.json(table_json)


def find_the_largest_subgraph(graph):
    result = graph.stronglyConnectedComponents(maxIter=10)
    largestComponent = result.orderBy('component').first()['component']
    vertices = result\
        .filter(result.component == largestComponent)\
        .select('id')
    return GraphFrame(vertices, graph.edges)


main()
