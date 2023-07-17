from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

spark = SparkSession.builder.appName("stedi-events").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

stediEventsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

stediEventsStreamingDF = stediEventsRawStreamingDF.selectExpr("cast(value as string) value")

customerRiskJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

stediEventsStreamingDF\
    .withColumn("value", from_json("value", customerRiskJSONSchema))\
    .select(col('value.*'))\
    .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

customerRiskStreamingDF.writeStream.outputMode("append").format("console")\
    .start().awaitTermination()
