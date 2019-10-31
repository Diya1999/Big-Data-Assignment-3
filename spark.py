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


if __name__ == "__main__":
	
	window_size, batch_size=sys.argv[1],sys.argv[2]
	conf=SparkConf()
	conf.setAppName("BigData")
	sc=SparkContext(conf=conf)

	ssc=StreamingContext(sc,int(batch_size))
	ssc.checkpoint("/home/kishan/Downloads/checkpoint")
	lines=ssc.socketTextStream("localhost",9009)
	words = lines.map(lambda line: line.split(";")[7])
	words = words.map(lambda x:x.split(','))
	hashtag=words.flatMap(lambda x:(x,1))
	windowedWordCounts = hashtag.reduceByKeyAndWindow(lambda x, y: x + y,lambda x, y:x-y,int(window_size),1 )
	windowedWordCounts.pprint()
	
	ssc.start()
	ssc.awaitTermination(25)
	ssc.stop()
