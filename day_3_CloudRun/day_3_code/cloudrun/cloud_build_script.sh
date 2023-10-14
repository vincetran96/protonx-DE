# export PROJECT_ID=durable-limiter-396112
# export IMAGE=simple-image
# export TAG=v1.0

# Instead of specify the above env vars, we source a file
# chmod +x ./.cloudrun_env
source ./.cloudrun_env

# Specify substitutions for parameters that are different
# from the cloudbuild.yaml file 
gcloud builds submit --config=cloudbuild.yaml --project=$PROJECT_ID 
# --substitutions=_PROJECT_ID=${PROJECT_ID},_IMAGE_NAME=${IMAGE},_TAG=${TAG}
