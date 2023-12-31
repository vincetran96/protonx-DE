import pyspark.sql.functions as F
from pyspark.sql import SparkSession, types

USERNAME = "USER_NAME_HERE"
PASSWORD = "PASSWORD_HERE"
if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Spark-Notebook").config(
        "spark.sql.streaming.checkpointLocation",
        "./checkpoint",
    ).config("spark.jars.packages",
             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1").config(
                 "java.security.auth.login.config",
                 "./kafka_jass.conf").getOrCreate())

    df_kafka_raw = (
        spark.readStream.format("kafka").option(
            "kafka.bootstrap.servers",
            "pkc-l7j7w.asia-east1.gcp.confluent.cloud:9092")
        # .option("spark.kafka.clusters.${cluster}.security.protocol","SASL_SSL")
        # .option("spark.kafka.clusters.${cluster}.sasl.token.mechanism","PLAIN")
        .option("kafka.security.protocol", "SASL_SSL").
        option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username={USERNAME} password={PASSWORD};'
        ).option("subscribe", "order-fixed-window-topic")
        # Tuỳ chọn subcribe topic theo pattern, có thể sub nhiều topic cùng lúc
        # .option("subscribePattern", "my-*-topic")
        .option("startingOffsets", "earliest")
        # Tuỳ chọn offset cho từng topic
        # .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
        # Chọn folder chứa checkpoint
        .load())

    schema = types.StructType([
        types.StructField("order_id", types.StringType(), True),
        types.StructField("user_id", types.StringType(), True),
        types.StructField("product_id", types.StringType(), True),
        types.StructField("quantity", types.IntegerType(), True),
    ])

    df_stream_order = (df_kafka_raw.withColumn(
        "value", F.from_json(F.col("value").cast(types.StringType()),
                             schema)).select(F.col("value.*"),
                                             F.col("timestamp")))

    windowedCounts = (df_stream_order.withWatermark(
        "timestamp", "30 seconds").groupBy(
            F.window(F.col("timestamp"), "30 minutes"),
            F.col("product_id")).agg(
                F.sum("quantity").alias("sum_quantity"),
                F.collect_set("order_id").alias("order_ids"),
                F.collect_set("timestamp").alias("timestamps"),
            ))

    stream = (windowedCounts.writeStream.format("console")
            .outputMode("update")
            .option("truncate", "false")
            .option("checkpointLocation","./checkpoint/kafka-fixed-window-checkpoint")
            .start())
    stream.awaitTermination()
