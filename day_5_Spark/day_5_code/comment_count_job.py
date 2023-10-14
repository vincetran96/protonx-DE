import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

import argparse

def main():
    parser = argparse.ArgumentParser(description=
                                    """PySpark Job read Amazon Review Data 
                                        and write number of helpful comment to GCS
                                    """)
    parser.add_argument("--input", required=True, help="Input Prefix")
    parser.add_argument("--output", required=True, help="Output Desitination")

    args = parser.parse_args()

    spark = ( 
        SparkSession.builder
                .master("yarn") 
                .appName('Aggreate Good Comment')
                .getOrCreate()
    )

    schema = types.StructType([
        types.StructField('marketplace', types.StringType(), True),
        types.StructField('customer_id', types.StringType(), True),
        types.StructField('review_id', types.StringType(), True),
        types.StructField('product_id', types.StringType(), True),
        types.StructField('product_parent', types.StringType(), True),
        types.StructField('product_category', types.StringType(), True),
        types.StructField('star_rating', types.IntegerType(), True),
        types.StructField('helpful_votes', types.IntegerType(), True),
        types.StructField('total_votes', types.IntegerType(), True),
        types.StructField('vine', types.LongType(), True),
        types.StructField('verified_purchase', types.LongType(), True),
        types.StructField('review_headline', types.StringType(), True),
        types.StructField('review_body', types.StringType(), True),
        types.StructField('review_date', types.StringType(), True)
    ])

    data_storage =  args.input + "/*"
    data_write =  args.output

    df = spark.read.schema(schema).parquet(data_storage)
    df = df.withColumn('review_date', F.to_date(F.col("review_date")))

    ( 
        df.select(
                F.col("product_id"),
                F.col("review_date"),
                F.col("star_rating"),
                F.col("helpful_votes")
            )
            .where((F.col("star_rating") >= 4 ) & (F.col("helpful_votes") >= 10 ))
            .groupBy(["product_id","review_date"])
            .count()
            .write.parquet(data_write,
                            mode = "overwrite")
    )


    # df.createOrReplaceTempView("df")
    # query = """

    # SELECT product_id,review_date,COUNT(star_rating)
    # FROM df 
    # WHERE star_rating >=4 and helpful_votes >= 10
    # GROUP BY product_id
    # """ 

    # spark.sql(query).write.parquet("gs://aws-review-data/write/report-count",
    #                         mode = "overwrite")

    spark.stop()

if __name__ == "__main__":
    main()