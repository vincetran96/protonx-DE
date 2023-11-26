PROJECT_ID=durable-limiter-396112
SPARK_JOB=pyspark_job/*.py
BUCKET_NAME=gs://trading-data-bucket-${PROJECT_ID}

gsutil -m cp pyspark_job/*.py  ${BUCKET_NAME}/pyspark_job