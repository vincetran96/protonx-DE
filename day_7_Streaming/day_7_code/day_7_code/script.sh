--- Socket Port 
nc -lk 9090

--- Create Topic 

python kafka_create_topic.py \
    --bootstrap-server localhost:9094



--- Fixed Window

python kafka_publish_event.py \
    --bootstrap-server localhost:9094 \
    --topic order-fixed-window-topic \
    --event-file ./data/order-event.json

--- Rolling Window

python kafka_publish_event.py \
    --bootstrap-server localhost:9094 \
    --topic order-rolling-window-topic \
    --event-file ./data/order-event-rolling.json


--- Session Window

python kafka_publish_event.py \
    --bootstrap-server localhost:9094 \
    --topic order-session-window-topic \
    --event-file ./data/order-event-session.json


--- Confluence

python kafka_publish_event_confluence.py \
    --config-file ./configuration.config \
    --topic order-fixed-window-topic \
    --event-file ./data/order-event.json
