PROJECT_ID=durable-limiter-396112
CPY_FILES=/Users/vince/protonx-DE/assignment_2/data/tick_data
DESTINATION=gs://trading-data-bucket-${PROJECT_ID}

gcloud storage buckets create ${DESTINATION}

gsutil -m cp -r ${CPY_FILES} ${DESTINATION}
