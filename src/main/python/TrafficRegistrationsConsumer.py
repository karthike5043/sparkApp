from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json,col,window

# create sparksession
spark = SparkSession.builder.appName('trafficRegistrationsDataApp') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1') \
    .getOrCreate()

## Read data from Kafka with the servername and Topic Name
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "INTERNAL://kafka:29092") \
    .option("subscribe", "trafficRegistrationPoints") \
    .option("startingOffsets", "earliest") \
    .load()
df.printSchema()


## Convert the Values in to Appropriate Schema Type
schema = StructType([ \
    StructField("name",StringType(),True), \
    StructField("location",StructType([StructField("county", StructType([StructField("name",StringType(),True)]), True), \
                                       StructField("coordinates",StructType([StructField("latLon",StructType([StructField("lon",FloatType(),True),StructField("lat",FloatType(),True)]),True)]),True)]),True), \
    StructField("id",StringType(),True), \
    StructField("trafficRegistrationType", StringType(), True), \
    StructField("direction", StructType([StructField("from",StringType(),True),StructField("to",StringType(),True)]), True)
])

## Count the number of Vehicles per Type per County
trafficDF = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")) \
    .select("data.name",col("data.location.county.name").alias("countyName"),"data.location.coordinates.latLon.lat","data.location.coordinates.latLon.lon", \
            "data.id","data.trafficRegistrationType","data.direction.from","data.direction.to") \
    .groupBy("countyName","trafficRegistrationType").count()

trafficDF.writeStream.format("console").outputMode("update").start().awaitTermination()