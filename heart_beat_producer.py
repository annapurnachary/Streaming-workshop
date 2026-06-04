import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name = 'green-trips'

# Send one record with a November timestamp to "push" the watermark
heartbeat = {
    "pulocationid": 1,
    "lpep_pickup_datetime": "2025-11-01 01:00:00",
    "trip_distance": 0.0,
    "total_amount": 0.0,
    "passenger_count": 1,
    "tip_amount": 0.0
}

producer.send(topic_name, value=json.dumps(heartbeat).encode('utf-8'))
producer.flush()
print("Heartbeat sent! Check the Flink UI for 'Records Sent' now.")
