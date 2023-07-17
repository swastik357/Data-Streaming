from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

redisServerSchema = StructType (
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", FloatType())
            ]))
        )
    ]
)

customerJSONSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

customerRiskJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("redis-server").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load() 

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(value as string) value")

redisServerStreamingDF.withColumn("value", from_json("value", redisServerSchema))\
            .select(col('value.existType'), col('value.Ch'),\
                    col('value.Incr'), col('value.zSetEntries'))\
            .createOrReplaceTempView("RedisSortedSet")

zSetEntriesEncodedStreamingDF=spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet")

zSetEntriesDecodedStreamingDF = zSetEntriesEncodedStreamingDF \
    .withColumn("customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
zSetEntriesDecodedStreamingDF \
    .withColumn("customer", from_json("customer", customerJSONSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    """
    SELECT 
    email, birthDay 
    FROM CustomerRecords 
    WHERE email IS NOT NULL AND BIRTHDAY IS NOT NULL
    """
)

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .select('email', split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
