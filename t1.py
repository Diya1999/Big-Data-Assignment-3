from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext


spark = SparkSession \
  .builder \
  .appName("CommonHash") \
  .getOrCreate()


# userSchema= StructType().add("word","string").add("id","integer")
userSchema= StructType().add("n","integer").add("id","integer").add("Lang","string")\
    .add("Date","string").add("Source","string").add("len","integer")\
        .add("Likes","integer").add("RT's","string").add("Hashtags","string").\
            add("UserMentionNames","string").add("UserMentionID","string").\
                add("Name","string").add("Place","string").add("Followers","string").\
                    add("Friends","string")
    

lines = spark \
    .readStream \
    .format("csv") \
    .option("sep", ";") \
    .schema(userSchema) \
    .load("hdfs://localhost:9000/stream")

  

# words = lines.select(
#    explode(
#        split(lines.value, ";")
#    ).alias("word")
# )


# wordCounts = lines.map(lambda line:line[7].split(",")).map(lambda x:(x,1)).groupByKey().count()
wordCounts=lines.groupBy("UserMentionID").count()


query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
