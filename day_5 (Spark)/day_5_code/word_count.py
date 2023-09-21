import pyspark
from pyspark.sql import SparkSession
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


    def count_word(row):
        review = row.review_body
        for word in review.split(" "):
            yield (word,1)

    def my_sum(x,y):
        return x + y

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
    rdd_review_body = df.select("review_body").rdd
    (
        rdd_review_body
            .flatMap(count_word)
            .reduceByKey(my_sum)
            .saveAsTextFile(data_write)
    )

if __name__ == "__main__":
    main()