PROJECT_ID=protonx-de-01
CPY_FILES=/Users/minh/Documents/GitHub/ProtonX/Assignment_2/data/tick_data
DESTINATION=gs://trading-data-bucket-${PROJECT_ID}

gsutil -m cp -r ${CPY_FILES} ${DESTINATION}
