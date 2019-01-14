# -*- coding: utf8 -*-
import datetime
from graphframes import *
from operator import add
from pyspark.sql import SQLContext, SparkSession, Row
import pyspark.sql.functions
#from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

APP_NAME = 'project'
SENDER = 0
RECEIVER = 1
VALUE = 2
TIMESTAMP = 3

def logger(message):
    recent_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print '[%s] %s' % (recent_time, message)


if __name__ == '__main__':

	spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
	sqlContext = SQLContext(spark)
	logger('Reading blockchain data...')
	#Load CSV
	CSV = 'gs://spark_bucket_1/bitcoin_dataset/btc_demo2.csv'
	dataset = spark.read.load(CSV, format='csv', header=False).rdd.cache()
	dataframeset = spark.createDataFrame(dataset)

	#logger('Thats show in Dataframe!')
	#dataframeset.show()

	#logger('Thats show in Pandas Dataframe!')
	#Change spark dataframe to pandas dataframe
	df2 = dataframeset.toPandas()
	#Drop the first row which is just column name like"DTime"
	df2 = df2.drop(range(0,1))
	#logger('Successfully drop!')
	#Reset column name 
	new_col = ['DTime','transaction_id','src','output_satoshi','dst']
	df2.columns = new_col
	print("OK!!!")


	#Filtering uncompleted data	including "NONE" or "NaN"
	logger('Thats DROP!')
	df3 = df2.dropna()
	df3.reset_index(drop=True,inplace=True)
	#print(df.dropna())


	#Create a dataframe including source address
	#logger('Thats create df4!')
	df4 = df3['src']
	df4 = df4.to_frame()
	new_col1 = ['id']
	df4.columns = new_col1
	#print(df4)

	#Create a dataframe including source address
	#logger('Thats create df5!')
	df5 = df3['dst']
	df5 = df5.to_frame()
	new_col2 = ['id']
	df5.columns = new_col2
	#print(df5)

	#Put all address into a dataframe and drop repeat address
	logger('Thats create merge!')
	df4 = df4.append(df5,ignore_index=True)
	df4.drop_duplicates(subset=['id'],keep='first',inplace=True)
	#print(df4)


	
	logger('Thats create GRAPH!')
	logger('Thats pop')
	dtime = df3.pop('DTime')
	tran = df3.pop('transaction_id')
	sato = df3.pop('output_satoshi')
	logger('Thats insert')
	df3.insert(2,'satoshi',sato)
	df3.insert(3,'DTime',dtime)
	df3.insert(4,'transaction_id',tran)
	#print(df3)
	#Create Graph
	#It seems that only spark dataframe can be put into graphframe
	logger('Thats create v/e')
	v = spark.createDataFrame(df4)#change back to spark df
	#df3.drop(range(0,700000),inplace=True)#only try a few data , not all
	e = spark.createDataFrame(df3)
	logger('Thats create g')
	g = GraphFrame(v, e)

	#ConnectedComponent
	con1 = g.connectedComponents().toPandas()
	con = con1.groupby('component').apply(len).rename("Frequency").to_frame()
	con = con.sort_values(by=['Frequency'],ascending=False)
	con.reset_index(inplace=True)
	print(con)
	#print(con.Frequency.max())
	#print(con1)
	con = con[con['Frequency']==con.Frequency.max()]
	con1 = con1[con1['component']==con['component'][0]]
	con1.pop('component')
	#print(con1)
	pd.merge(df3,con1,left_on='src',right_on='id',how='inner')
	#df3 = df3[df3['src']==con1['id']]
	df3.reset_index(drop=True,inplace=True)
	print(df3)
	

	v = spark.createDataFrame(con1)
	e = spark.createDataFrame(df3)
	g = GraphFrame(v, e)

	g.connectedComponents().show()
	pr = g.pageRank(resetProbability=0.15, maxIter = 5)
	pr.vertices.orderBy(pr.vertices.pagerank.desc()).show()
	

	#g.degrees.orderBy(g.degrees.degree.desc()).show()

	#print(vp)
	#con = spark.createDataFrame(con)
	
	
	#con = con.filter(con.component<con.component.max())
	
	#g.vertices = g.connectedComponents()  #seems its wrong because they are diffrent type
	#g.vertices.drop(indexof('component' in con . frequency < 10))  #the issue is how to drop address that not in big component(frequency<10)
	
	#Pagerank  #the problem is how to analysis those pageranks
	#pr = g.pageRank(resetProbability=0.15, tol=0.01)
	#pr = g.pageRank(resetProbability=0.15, maxIter = 5)
	#pr.vertices.orderBy(pr.vertices.pagerank.desc()).show()
	#pr.vertices.filter('pagerank>0.15').show()
	#pr.edges.filter('weight>0.07').show()
	#g.vertices.show()
	#g.edges.show()
	spark.stop()
	logger('Done!')


