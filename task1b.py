from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import explode,array_max
from pyspark.sql.functions import *
from pyspark.sql.functions import split,flatten
from pyspark.sql.types import StructType

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import Row
from pyspark import SparkContext


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
    .add("Followers", "integer")
    .add("Friends", "integer")
)


lines = (
    spark.readStream.format("csv")
    .option("sep", ";")
    .schema(userSchema)
    .load("hdfs://localhost:9000/stream")
)

df = lines.withColumn("ratio",lines.Followers/lines.Friends)
df = df.select("Name","ratio").agg({"ratio":"max"})
# df = df.select("Name","ratio").where("ratio"==r)
# df =  





query = df.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
