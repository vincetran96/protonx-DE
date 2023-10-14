import pyspark.sql.functions as F
from delta import *
from delta.tables import *
from pyspark.sql import SparkSession, types

if __name__ == "__main__":
    artifact = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "org.apache.spark:spark-avro_2.12:3.4.1",
        # "io.delta:delta-core_2.12:2.1.0"
    ]
    spark_builder = (
        SparkSession.builder
        .appName("Spark-Notebook")
        # .config("spark.sql.streaming.checkpointLocation","./checkpoint",)
        # .config("spark.jars.packages",",".join(artifact))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .getOrCreate()
    )

    spark = configure_spark_with_delta_pip(spark_builder,
                                        extra_packages=artifact).getOrCreate()

    print(f"Version {spark.sparkContext.version}")

    df_kafka_raw = (
        spark.readStream.format("kafka").option(
            "kafka.bootstrap.servers",
            "localhost:9094,localhost:19094").option(
                "subscribe", "order-fixed-window-topic")
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

    df_stream_order = (
        df_kafka_raw.withColumn(
            "value", F.from_json(F.col("value").cast(types.StringType()),
                                schema)).select(F.col("value.*"),
                                                F.col("timestamp"))
    )

    windowedSum = (
        df_stream_order.withWatermark(
        "timestamp", "30 seconds").groupBy(
            F.window(F.col("timestamp"), "30 minutes"),
            F.col("product_id")).agg(
                F.sum("quantity").alias("sum_quantity"),
                # F.collect_set("order_id").alias("order_ids"),
                # F.collect_set("timestamp").alias("timestamps"),
    ))
    # .select(
    #     F.col("window.*"), 
    #     F.col("product_id"),
    #     F.col("sum_quantity"),)

    stream = (
        windowedSum
            .writeStream
            .format("delta")
            # .outputMode("update")
            # .outputMode("append")
            .option("checkpointLocation","./checkpoint/write-to-file-checkpoint")
            .start(path= "outputs/delta-table")
        )
    stream.awaitTermination()
