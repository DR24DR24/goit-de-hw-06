from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType,StringType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Пакет, необхідний для читання Kafka зі Spark
# os.environ[
#     'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
# '--packages org.apache.spark:org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'



# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
         .getOrCreate())



alerts_df = spark.read.csv(
    "alerts_conditions.csv",
    header=True,
    inferSchema=True
)

from pyspark.sql.functions import col, when

alerts_df = alerts_df.select(
    col("id"),
    when(col("humidity_min") == -999, None).otherwise(col("humidity_min")).alias("humidity_min"),
    when(col("humidity_max") == -999, None).otherwise(col("humidity_max")).alias("humidity_max"),
    when(col("temperature_min") == -999, None).otherwise(col("temperature_min")).alias("temperature_min"),
    when(col("temperature_max") == -999, None).otherwise(col("temperature_max")).alias("temperature_max"),
    col("code"),
    col("message")
)

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "rogalev_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50") \
    .load()







# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON. 
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("sensorType", StringType(), True)
])

# Маніпуляції з даними
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("value", col("value_json.value")) \
    .withColumn("sensortype", col("value_json.sensorType")) \
    .drop("value_json", "value_deserialized")\
    .withWatermark("timestamp", "10 seconds") 
    
 
from pyspark.sql.functions import expr, col



# Оконная агрегация + pivot + join с условиями + формирование JSON
alerts_stream_df = (clean_df
    .groupBy(window("timestamp", "1 minute", "30 seconds"), "sensorType")
    .agg(avg("value").alias("avg_value"))
    .groupBy("window")
    .pivot("sensorType", ["temperature", "humidity"])
    .agg(expr("first(avg_value)"))
    .withColumnRenamed("temperature", "t_avg")
    .withColumnRenamed("humidity", "h_avg")
    .crossJoin(alerts_df)
    .filter(
        ((col("temperature_min").isNotNull() & (col("t_avg") >col("temperature_min"))) &
         (col("temperature_max").isNotNull() & (col("t_avg") < col("temperature_max")))) |
        ((col("humidity_min").isNotNull() & (col("h_avg") > col("humidity_min"))) &
         (col("humidity_max").isNotNull() & (col("h_avg") < col("humidity_max"))))
    )
    .select(
        col("window.start").cast("string").alias("key"),
        to_json(struct(
                        struct(
                            col("window.start").alias('start'), 
                            col("window.end").alias('end')
                              ).alias('window')\
                       ,"t_avg", "h_avg", "code", "message"
                       ,(timestamp_seconds(unix_timestamp())).alias("timestamp")
                       )
                ).alias("value")
            )    
)


alerts_df.printSchema()
alerts_stream_df.printSchema()
clean_df.printSchema()
print(alerts_df.show(5, False))

# Запись алертов в Kafka
query_kafka = alerts_stream_df.writeStream \
    .trigger(processingTime='15 seconds') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "rogalev_temperature_alerts") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';"
    ) \
    .option("checkpointLocation", "/tmp/checkpoints-alerts-8") \
    .outputMode("update")\
    .start() 


# Console sink
query_console = (
    alerts_stream_df.writeStream
        .trigger(processingTime='15 seconds')
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
)

# Ожидание завершения
query_kafka.awaitTermination()
query_console.awaitTermination()
