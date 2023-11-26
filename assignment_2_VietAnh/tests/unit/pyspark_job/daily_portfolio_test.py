from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark_job.daily_portfolio import compute_ohlc, compute_portfolio_value_in_usdt
# from pyspark_job.daily_pnl import compute_ohlc, compute_portfolio_value_in_usdt


def assertDataFrameEqual(df_output, df_expected):
    assert df_output.schema == df_expected.schema
    output_list = df_output.collect()
    excepted_list = df_expected.collect()

    assert len(output_list) == len(excepted_list)
    for output, expect in zip(output_list, excepted_list):
        assert output == expect


def test_ohlc(spark):
    tick_data = [
        ("ETH",10.0,10.0,10.0,1698710400000), # 2023-10-31 00:00:00
        ("ETH",9.9,10.0,10.0,1698711000000), # 2023-10-31 00:10:00
        ("ETH",20.0,10.0,10.0,1698713400000), # 2023-10-31 00:50:00
        ("ETH",11.0,10.0,10.0,1698713999000), # 2023-10-31 00:59:59
        ("BTC",10000.0,1.0,10.0,1698710400000), # 2023-10-31 00:00:00
        ("BTC",99999.0,1.0,10.0,1698711000000), # 2023-10-31 00:10:00
        ("BTC",9999.9,1.0,10.0,1698713400000), # 2023-10-31 00:10:00
        ("BTC",1100.0,10.0,1.0,1698713999000), # 2023-10-31 00:20:00
        ("BTC",1000.0,10.0,1.0,1698714000000), # 2023-10-31 01:00:00
    ]

    trade_schema = types.StructType([
        types.StructField("symbol", types.StringType(), True),
        types.StructField("price", types.FloatType(), True),
        types.StructField("qty", types.FloatType(), True),
        types.StructField("base_qty", types.FloatType(), True),
        types.StructField("time", types.LongType(), True),
    ])

    expected_output = [
        (datetime(2023,10,31,7,0,0),"ETH", 10.0, 20.0, 9.9, 11.0,40.0),
        (datetime(2023,10,31,7,0,0),"BTC", 10000.0, 99999.0, 1100.0, 1100.0, 13.0),
        (datetime(2023,10,31,8,0,0),"BTC",1000.0,1000.0,1000.0,1000.0,10.0), 
    ]

    output_schema = types.StructType([
        types.StructField("time", types.TimestampType(), True),
        types.StructField("symbol", types.StringType(), True),
        types.StructField("open", types.FloatType(), True),
        types.StructField("high", types.FloatType(), True),
        types.StructField("low", types.FloatType(), True),
        types.StructField("close", types.FloatType(), True),
        types.StructField("volume", types.DoubleType(), True),
    ])

    df_tick = spark.createDataFrame(tick_data, schema=trade_schema)

    df_expected_output = spark.createDataFrame(
        expected_output, schema=output_schema
    )

    df_result = compute_ohlc(df_tick, window_duration="1 hour")

    assertDataFrameEqual(df_result, df_expected_output)


def test_pnl(spark):
    input_ohlc = [
        (datetime(2023,10,31,0,0,0),"ETH", 10.0, 20.0, 9.9, 11.0, 40.0),
        (datetime(2023,10,31,0,0,0),"BTC", 10000.0, 99999.0, 1100.0, 1100.0, 13.0),
    ]

    ohlc_schema = types.StructType([
        types.StructField("time", types.TimestampType(), True),
        types.StructField("symbol", types.StringType(), True),
        types.StructField("open", types.FloatType(), True),
        types.StructField("high", types.FloatType(), True),
        types.StructField("low", types.FloatType(), True),
        types.StructField("close", types.FloatType(), True),
        types.StructField("volume", types.DoubleType(), True),
    ])

    input_user_position = [
        (1, 'ETH',1.3,  datetime(2023,10,31,7,0,0)),
        (1, 'BTC',0.01, datetime(2023,10,31,7,0,0)),
        (1, 'USDT',20.0, datetime(2023,10,31,7,0,0)),
        (2, 'ETH',1.0, datetime(2023,10,31,7,0,0)),
        (2, 'USDT',100.0, datetime(2023,10,31,7,0,0))
    ]

    user_position_schema = types.StructType([
        types.StructField("user_id", types.IntegerType(), True),
        types.StructField("symbol", types.StringType(), True),
        types.StructField("position", types.FloatType(), True),
        types.StructField("last_updated", types.DateType(), True),
    ])

    expected_output = [
        (1,45.30),
        (2,111.0),
    ]

    output_schema = types.StructType([
        types.StructField("user_id", types.IntegerType(), True),
        types.StructField("total_value", types.DoubleType(), True),
    ])

    df_ohlc = spark.createDataFrame(input_ohlc, schema=ohlc_schema)
    df_user_position = spark.createDataFrame(
        input_user_position, schema=user_position_schema
    )

    df_user_position.show()
    df_expected_output = spark.createDataFrame(
        expected_output, schema=output_schema
    )
    df_portfolio = compute_portfolio_value_in_usdt(df_ohlc, df_user_position)


    # Round value to compare with expected output
    df_portfolio = df_portfolio.withColumn("total_value", 
                                        F.round(F.col("total_value"),2))

    assertDataFrameEqual(df_portfolio,df_expected_output)