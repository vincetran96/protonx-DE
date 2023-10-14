import time

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
        .config("spark.streaming.stopGracefullyOnShutdown","true")
        .getOrCreate()
    )

    df_kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9094,localhost:19094")
        .option("subscribe", "order-session-window-topic")
        # Tuỳ chọn subcribe topic theo pattern, có thể sub nhiều topic cùng lúc
        # .option("subscribePattern", "my-*-topic")
        .option("consumer.timeout.ms", "10000")
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

    df_stream_order = ( 
        df_kafka_raw.withColumn(
            "value", 
            F.from_json(
                F.col("value").cast(types.StringType()), 
                schema)
        ).select(F.col("value.*"), F.col("timestamp"))
    )
    
    session_window_expr = F.session_window(F.col("timestamp"),"10 minutes")


    windowedCounts = (
        df_stream_order.withWatermark("timestamp", "30 seconds")
        .groupBy(
            session_window_expr, 
            F.col("user_id"))
        .agg(
            F.collect_set("order_id").alias("order_ids"),
            F.collect_set("timestamp").alias("timestamps"),
        )
    )

    windowedCounts.printSchema()

    stream = (
        windowedCounts.writeStream
        .format("console")
        .outputMode("append")
        # .outputMode("update")
        .option("truncate", "false")
        # Chọn folder chứa checkpoint
        .option(
            "checkpointLocation",
            "./checkpoint/session-window-checkpoint",
        )
        .start()
    )
    
    
    stream.awaitTermination()
    # stream.stop(True,True)
    # print("Stream is stopped!")
    # print("Stream is stopped!")
    # print("Stream is stopped!")
