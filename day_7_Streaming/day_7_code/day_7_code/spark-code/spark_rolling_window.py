import pyspark.sql.functions as F
from pyspark.sql import SparkSession, types

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Spark-Notebook")
        .config(
            "spark.sql.streaming.checkpointLocation",
            "./checkpoint",
        )
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )
        .getOrCreate()
    )

    df_kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9094,localhost:19094")
        .option("subscribe", "order-rolling-window-topic")
        # Tuỳ chọn subcribe topic theo pattern, có thể sub nhiều topic cùng lúc
        # .option("subscribePattern", "my-*-topic")
        .option("startingOffsets", "earliest")
        # Tuỳ chọn offset cho từng topic
        # .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
        .load()
    )

    schema = types.StructType(
        [
            types.StructField("order_id", types.StringType(), True),
            types.StructField("user_id", types.StringType(), True),
            types.StructField("product_id", types.StringType(), True),
            types.StructField("quantity", types.IntegerType(), True),
        ]
    )

    df_stream_order = df_kafka_raw.withColumn(
        "value", F.from_json(F.col("value").cast(types.StringType()), schema)
    ).select(F.col("value.*"), F.col("timestamp"))

    windowedCounts = (
        df_stream_order
        .withWatermark("timestamp", "1 seconds")
        .groupBy(
                F.window(F.col("timestamp"), 
                         "2 minutes",
                         "1 minutes"), 
                F.col("product_id")
        )
        .agg(
            F.sum("quantity").alias("sum_quantity"),
            F.collect_set("order_id").alias("order_ids"),
            F.collect_set("timestamp").alias("timestamps"),
        )
    )

    stream = (
        windowedCounts.writeStream.format("console")
        .outputMode("update")
        # .outputMode("append")
        .option("truncate", "false")
        # Chọn folder chứa checkpoint
        .option(
            "checkpointLocation",
            "./checkpoint/rolling-window-checkpoint",
        )
        .start()
    )
    stream.awaitTermination()
