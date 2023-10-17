import pyspark.sql.functions as F
from pyspark.sql import SparkSession, types

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("Spark-Notebook")
        .getOrCreate()
    )

    stream = (
        spark
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9090)
        .load()
        .writeStream
        .format("console")
        # .trigger(once=True)
        # .trigger(continuous="1 second")
        .trigger(processingTime="5 seconds")
        .outputMode("append")
        .start()
    )

    stream.awaitTermination()
