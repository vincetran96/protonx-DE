import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def compute_ohlc(df_tick: DataFrame, window_duration="1 hour") -> DataFrame:
    """
    Hàm này tính toán các giá trị Open, High, Low, Close (OHLC)
    từ dữ liệu tick.

    Parameters:
    df_tick (DataFrame):
            DataFrame dữ liệu tick.
            Mỗi dòng đại diện cho một giá coin
            tại một thời điểm cụ thể.

    window_duration(str): Độ dài của mỗi khoảng thời gian.

    Returns:
    DataFrame: Một DataFrame với các giá trị OHLC
                cho mỗi khoảng thời gian.

    >>> df_tick
    +------+-------+----+--------+-------------------+
    |symbol|  price| qty|base_qty|               time|
    +------+-------+----+--------+-------------------+
    |   ETH|   10.0|10.0|    10.0|2023-10-31 00:00:00|
    |   ETH|    9.9|10.0|    10.0|2023-10-31 00:10:00|
    |   ETH|   20.0|10.0|    10.0|2023-10-31 00:50:00|
    |   ETH|   11.0|10.0|    10.0|2023-10-31 00:59:59|
    |   ETH|   11.0|10.0|    10.0|2023-10-31 01:00:00|
    +------+-------+----+--------+-------------------+

    >>> compute_ohlc(df_tick, window_duration = "1 hour")
    +-------------------+------+-------+-------+------+------+------+
    |               time|symbol|   open|   high|   low| close|volume|
    +-------------------+------+-------+-------+------+------+------+
    |2023-10-31 00:00:00|   ETH|   10.0|   20.0|   9.9|  11.0|  40.0|
    |2023-10-31 01:00:00|   ETH|   11.0|   11.0|  11.0|  11.0|  10.0|
    +-------------------+------+-------+-------+------+------+------+

    """
    # Convert time column from int to datetime type first
    df_ohlc = df_tick.withColumn(
        "time", F.from_unixtime(F.col("time") / 1000)
    )

    df_ohlc = df_ohlc \
        .groupBy(F.window("time", window_duration), "symbol") \
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("qty").alias("volume")
        ) \
        .select(
            F.col("window").start.alias("time"),
            "symbol",
            "open", "high", "low", "close", "volume"
        )

    return df_ohlc


def compute_portfolio_value_in_usdt(
    df_ohlc: DataFrame, df_user: DataFrame
) -> DataFrame:
    """
    Hàm này tính toán tổng giá trị danh mục
    cho người dùng dựa trên dữ liệu OHLC.

    Parameters:
    df_ohlc (DataFrame): DataFrame dữ liệu OHLC.     
                        Mỗi hàng đại diện cho một OHLC của symbol.
    df_user (DataFrame): DataFrame dữ liệu người dùng. 
                        Mỗi hàng đại diện cho 1 vị thế vào cuối ngày.

    Returns:
    DataFrame: Một DataFrame chứa tổng giá trị danh mục
                vào cuối ngày cho của mỗi user.

    >>> df_ohlc
    +-------------------+------+-------+-------+------+------+------+
    |               time|symbol|   open|   high|   low| close|volume|
    +-------------------+------+-------+-------+------+------+------+
    |2023-10-30 01:00:00|   BNB|   10.0|   20.0|  10.0| 100.5|  40.0|
    |2023-10-30 23:00:00|   BNB|   10.0|   20.0|   9.9| 227.5|  40.0|
    +-------------------+------+-------+-------+------+------+------+
    >>> df_user
    +-------+------+----------+-------------------+
    |user_id|symbol|position  |       last_updated|
    +-------+------+----------+-------------------+
    |      1|  USDT|    1000.0|2023-10-30 00:00:00|
    |      1|   BNB|       1.0|2023-10-30 00:00:00|
    +-------+------+----------+-------------------+
    >>> compute_portfolio_value_in_usdt(df_ohlc,df_user)

    +-------+-----------+
    |user_id|total_value|
    +-------+-----------+
    |      1|     1227.5|
    +-------+-----------+

    """
    window = Window.partitionBy("symbol").orderBy(F.col("time").desc())

    sym_cl_price = df_ohlc \
        .withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .select(F.col("symbol"), F.col("close"))

    df_user_port = df_user.join(
        sym_cl_price, on="symbol", how="left"
    )
    df_user_port = df_user_port \
        .withColumn(
            "usdt_value",
            F.when(F.col("symbol") == "USDT", F.col("position"))
            .otherwise(F.col("position") * F.col("close"))
        )

    df_user_port.show()

    df_user_port = df_user_port \
        .groupBy("user_id") \
        .agg(
            F.sum("usdt_value").alias("total_value")
        )

    return df_user_port


def main():
    """
        --input-tick
        --input-user
        --output-ohlc
        --output-portfolio
    """
    parser = argparse.ArgumentParser(
        description="""
            PySpark Job read User + Tick Data from GCS,
            Then compute total portfolio value and OHLC,
        """
    )

    parser.add_argument(
        "--input-tick",
        dest="input_tick",
        required=True
    )
    parser.add_argument(
        "--input-user",
        dest="input_user",
        required=True
    )
    parser.add_argument(
        "--output-ohlc",
        dest="output_ohlc",
        required=True
    )
    parser.add_argument(
        "--output-portfolio",
        dest="output_portfolio",
        required=True
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName('Daily PnL Job')
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    trade_schema = types.StructType([
        types.StructField("symbol", types.StringType(), True),
        types.StructField("price", types.FloatType(), True),
        types.StructField("qty", types.FloatType(), True),
        types.StructField("base_qty", types.FloatType(), True),
        types.StructField("time", types.LongType(), True),
    ])

    user_position_schema = types.StructType([
        types.StructField("user_id", types.StringType(), True),
        types.StructField("symbol", types.StringType(), True),
        types.StructField("position", types.FloatType(), True),
        types.StructField("last_updated", types.DateType(), True),
    ])

    tick_files = args.input_tick + "/*"
    user_files = args.input_user + "/*"
    output_ohlc = args.output_ohlc
    output_portfolio = args.output_portfolio

    df_tick = (
        spark.read
        .format("avro")
        .schema(trade_schema)
        .load(tick_files)
    )

    df_user = (
        spark.read
        .format("json")
        .schema(user_position_schema)
        .load(user_files)
    )

    df_ohlc = compute_ohlc(df_tick, window_duration="1 hour")
    df_user_pnl = compute_portfolio_value_in_usdt(df_ohlc, df_user)

    (
        df_ohlc.write
        .format("parquet")
        .mode('overwrite')
        .partitionBy("symbol")
        .option("compression","snappy")
        .save(output_ohlc)
    )

    (
        df_user_pnl
        .write
        .format("parquet")
        .mode('overwrite')
        .option("compression","snappy")
        .save(output_portfolio)
    )

    spark.stop()


if __name__ == "__main__":
    main()
