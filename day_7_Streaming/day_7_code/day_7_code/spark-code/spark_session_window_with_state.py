from datetime import timedelta
from typing import Iterator, Tuple

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, types
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout


def state_session_window_func(
    keys: Tuple[str], 
    pdfs: Iterator[pd.DataFrame], 
    state: GroupState
) -> Iterator[pd.DataFrame]:
    
    for pdf in pdfs:
        user_id = keys[0]
        pdf = pdf.sort_values(by=['timestamp'])
        for _,row in pdf.iterrows():
            timestamp = row['timestamp']
            order_id = row['order_id']
            quantity = row['quantity']

            if not state.exists:
                session_id = f"{user_id}-{timestamp.to_pydatetime().strftime('%Y%m%d%H%M%S')}"
                
                state.update((session_id,session_id,timestamp,))

            else:
                (session_id,old_session_id,end_ss) = state.get

                if (timestamp - end_ss) >= timedelta(minutes = 10):
                    # Update new session_id
                    old_session_id = session_id
                    session_id = f"{user_id}-{timestamp.to_pydatetime().strftime('%Y%m%d%H%M%S')}"

                state.update((session_id,old_session_id,timestamp,))


            yield pd.DataFrame(
                {
                    "session_id" : [session_id],
                    "user_id": [user_id],
                    "order_id": [order_id],
                    "quantity": [quantity],
                    "timestamp": [timestamp]
                }
            )

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

    # Define Schema 
    stream_order_schema = types.StructType(
        [
            types.StructField("order_id", types.StringType(), True),
            types.StructField("user_id", types.StringType(), True),
            types.StructField("product_id", types.StringType(), True),
            types.StructField("quantity", types.IntegerType(), True),
        ]
    )
    
    session_schema = types.StructType(
        [  
            types.StructField("session_id", types.StringType(), True ),
            types.StructField("user_id", types.StringType(), True),
            types.StructField("order_id", types.StringType(), True),
            types.StructField("quantity", types.IntegerType(), True),
            types.StructField("timestamp", types.TimestampType(), True)
        ]
    )
    
    state_schema =  types.StructType(
        [
            types.StructField("session_id", types.StringType(), True ),
            types.StructField("old_session_id", types.StringType(), True ),
            types.StructField("end_ss", types.TimestampType(), True ),
        ])


    df_kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9094,localhost:19094")
        .option("subscribe", "order-session-window-topic")
        # Tuỳ chọn subcribe topic theo pattern, có thể sub nhiều topic cùng lúc
        # .option("subscribePattern", "my-*-topic")
        # Tuỳ chọn offset cho từng topic
        # .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
        .load()
    )

    df_stream_order = ( 
        df_kafka_raw.withColumn(
            "value", 
            F.from_json(
                F.col("value").cast(types.StringType()), 
                stream_order_schema)
        ).select(F.col("value.*"), F.col("timestamp"))
    )
    
    

    sessions = ( 
        df_stream_order
        .groupBy(
            F.col("user_id"),
        )
        .applyInPandasWithState(
            state_session_window_func,
            session_schema,
            state_schema,
            "append",
            GroupStateTimeout.NoTimeout,
        )
        # .withWatermark("timestamp", "30 seconds")
        .groupBy(
            F.col("session_id"), 
            F.col("user_id")).agg(
                F.sum("quantity").alias("sum_quantity"),
                F.collect_set("order_id").alias("order_ids"),
                F.collect_set("timestamp").alias("timestamps"),
            )
    )

    stream = (
        sessions.writeStream
        # .trigger(processingTime="10 seconds")
        .format("console")
        # .outputMode("append")
        .outputMode("update")
        .option("truncate", "false")
        # Chọn folder chứa checkpoint
        .option(
            "checkpointLocation",
            "./checkpoint/session-window-checkpoint-state",
        )
        .start()
    )
    
    stream.awaitTermination()
