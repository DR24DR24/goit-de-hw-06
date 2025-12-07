from pyspark.sql import SparkSession

import pyspark
print("pyspark:", pyspark.__version__)

spark = (
    SparkSession.builder
        .appName("KafkaStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
)

print("Spark version:", spark.version)

spark.stop()
