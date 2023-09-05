# export PROJECT_ID=durable-limiter-396112
# export IMAGE=simple-image
# export TAG=v1.0
# export JOB_NAME=simple-job 
# export SERVICE_ACCOUNT=batch-job-service-account@durable-limiter-396112.iam.gserviceaccount.com

    # Instead of specify the above env vars, we source a file
    # chmod +x ./.cloudrun_env
    source ./.cloudrun_env

gcloud run jobs create ${JOB_NAME} \
    --region asia-east1 \
    --image gcr.io/${PROJECT_ID}/${IMAGE}:${TAG} \
    --service-account ${SERVICE_ACCOUNT} \
    --project ${PROJECT_ID}
