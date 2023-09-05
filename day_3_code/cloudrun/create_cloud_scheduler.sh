export PROJECT_ID=test-app-309909
export IMAGE=simple-image
export TAG=v1.0
export SCHEDULER_NAME=simple-scheduler
export JOB_NAME=simple-job
export CLOUD_RUN_REGION=asia-east1
export SCHEDULER_REGION=asia-east1
export SERVICE_ACCOUNT=batch-job-service-account@test-app-309909.iam.gserviceaccount.com


gcloud scheduler jobs create http ${SCHEDULER_NAME}\
        --location ${SCHEDULER_REGION} \
        --schedule="* * * * *" \
        --uri="https://${CLOUD_RUN_REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
        --http-method POST \
        --oidc-service-account-email ${SERVICE_ACCOUNT}

