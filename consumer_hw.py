from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    # Use a completely random group ID to ensure we read from message 0
    group_id='verify_count_999', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    # Stop if no new messages arrive for 5 seconds
    consumer_timeout_ms=5000 
)

count = 0
total_processed = 0

print("Starting full count...")
for message in consumer:
    total_processed += 1
    ride = message.value
    # Use .get() to avoid KeyErrors
    if float(ride.get('trip_distance', 0)) > 5.0:
        count += 1

print(f"Total messages read from topic: {total_processed}")
print(f"Trips with distance > 5.0: {count}")

