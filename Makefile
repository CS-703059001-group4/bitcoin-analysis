run:
	gcloud dataproc jobs submit pyspark \
		--cluster cluster-9378\
		--properties spark.jars.packages='graphframes:graphframes:0.2.0-spark2.0-s_2.11' \
		--jars 'gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar,gs://cs703059001/lib/hadoop-io.jar' \
		try.py
