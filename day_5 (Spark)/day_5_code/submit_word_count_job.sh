CLUSTER_NAME=cluster-4f6d
REGION=asia-east1
INPUT="gs://aws-review-data/read"
OUTPUT="gs://aws-review-data/write/word-count"

gcloud dataproc jobs submit pyspark \
    --cluster=${CLUSTER_NAME} \
    --region=${REGION} \
    word_count.py \
    -- \
        --input=${INPUT} \
        --output=${OUTPUT}