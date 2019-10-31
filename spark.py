#import findspark
#findspark.init()
from __future__ import print_function
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			sql_context = get_sql_context_instance(rdd.context)
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			hashtags_df = sql_context.createDataFrame(row_rdd)
			hashtags_df.registerTempTable("hashtags")
			hashtag_counts_df = sql_context.sql("select tweetid, no_of_tweets from hashtags")
			hashtag_counts_df.show()
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)

if __name__ == "__main__":
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,2)
	#ssc.checkpoint("/checkpoint_BIGDATA")
	lines=ssc.socketTextStream("localhost",9009)
	words = lines.map(lambda line: line.split(";")[7])
	words = words.map(lambda x:x.split(','))
	hashtag=words.flatMap(lambda x:(x,1))
	windowedWordCounts = hashtag.reduceByKeyAndWindow(lambda x, y: x + y, 20, 2)
	windowedWordCounts.pprint()
	#dataStream.pprint()
	#tweet=dataStream.map(tmp)'''
	'''lines = spark\
        .readStream\
        .format('socket')\
        .option('localhost', host)\
        .option('9009', port)\
        .option('includeTimestamp', 'true')\
        .load()'''

	# Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows
	
		# OR
		#tweet=dataStream.map(lambda w:(w.split(';')[0],1))
		#tweet.pprint()
		#count=tweet.reduceByKey(lambda x,y:x+y)
		#count.pprint()

		#TO maintain state
		 #totalcount=tweet.updateStateByKey(aggregate_tweets_count)
		 #totalcount.pprint()

		#To Perform operation on each RDD
		 #totalcount.foreachRDD(process_rdd)

	ssc.start()
	ssc.awaitTermination(12)
	ssc.stop()
