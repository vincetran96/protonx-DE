
# Run Python 
python python_producer.py --bootstrap-server localhost:9094 --topic my-first-topic
python python_consumer.py --bootstrap-server localhost:9094 --topic my-first-topic --consumer-group hello


python python_consumer_read_from_offset.py \
    --bootstrap-server localhost:9094 \
    --topic my-first-topic \
    --consumer-group hello \
    --partition 1 \
    --offset 0


python python_producer_cloud.py \
    --config-file ./client.properties \
    --topic my-first-topic

python python_consumer_cloud.py \
    --config-file ./client.properties \
    --topic my-first-topic \
    --consumer-group hello
