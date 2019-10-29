from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.sql.functions import split,flatten
from pyspark.sql.types import StructType

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import Row
from pyspark import SparkContext

def process_row(row):
    pass


spark = SparkSession.builder.appName("CommonHash").getOrCreate()


# userSchema= StructType().add("word","string").add("id","integer")
userSchema = (
    StructType()
    .add("id", "integer")
    .add("Lang", "string")
    .add("Date", "string")
    .add("Source", "string")
    .add("len", "integer")
    .add("Likes", "integer")
    .add("RT's", "string")
    .add("Hashtags", "string")
    .add("UserMentionNames", "string")
    .add("UserMentionID", "string")
    .add("Name", "string")
    .add("Place", "string")
    .add("Followers", "string")
    .add("Friends", "string")
)


lines = (
    spark.readStream.format("csv")
    .option("sep", ";")
    .schema(userSchema)
    .load("hdfs://localhost:9000/stream")
)


# words = lines.select(
#    explode(
#        split(lines.value, ";")
#    ).alias("word")
# )


# wordCounts = lines.select(split(lines.Hashtags,",").alias('sp').flatten(wordCounts.sp)
# wordCounts = flatten(wordCounts.sp).alias("F")

# wordCounts = flatten(wordCounts.sp).alias("F")
# wordCounts = wordCounts.select("F")

wordCounts = lines.select(explode(split(lines.Hashtags,",")).alias("sp"))
wordCounts = wordCounts.groupby("sp").count().sort("count",ascending=False)
# wordCounts = sort_array(wordCounts.count)
# wordCounts = wordCounts.select(sorted(wordCounts.groupBy("sp").agg({"count":"count"})))

# wordCounts = 
#     .map(lambda word: (word, 1))
#     .reduceByKey(add)
# )
# wordCounts = wordCounts.collect()


# wordCounts=lines.groupBy("UserMentionID").count()

# wordCounts=lines.select("Hashtags").count()

# query = wordCounts.writeStream.foreach(process_row).start()
# wordCounts = wordCounts.RDD

query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
