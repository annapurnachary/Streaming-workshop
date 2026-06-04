import dataclasses
import json
import sys
import time
from pathlib import Path

# Fix: Use raw string for Windows paths to avoid escape character errors
url = "../data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 
    'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
]

import pandas as pd
from kafka import KafkaProducer
# Assuming models_hw.py is in the parent directory as per your sys.path logic
sys.path.insert(0, str(Path(__file__).parent.parent))
from models_hw import Ride, ride_from_row

# 1. Load the entire dataset
df = pd.read_parquet(url, columns=columns)

df = df.fillna({
    'PULocationID': 0,
    'DOLocationID': 0,
    'passenger_count': 0,
    'trip_distance': 0.0,
    'tip_amount': 0.0,
    'total_amount': 0.0,
    'lpep_pickup_datetime': pd.Timestamp('1970-01-01'), # Logical default
    'lpep_dropoff_datetime': pd.Timestamp('1970-01-01')
})

df['PULocationID'] = df['PULocationID'].astype(int)
df['DOLocationID'] = df['DOLocationID'].astype(int)
df['passenger_count'] = df['passenger_count'].astype(int)

df.columns = [c.lower() for c in df.columns]

# 2. Optimized Serializer: Handle datetime objects
def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    # default=str handles datetime, UUID, and other non-JSON types
    json_str = json.dumps(ride_dict, default=str) 
    return json_str.encode('utf-8')


server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer,
    # 3. Tuning for high throughput
    acks=1,              # Faster than 'all'
    linger_ms=10,        # Buffers messages for 10ms to send in batches
    #compression_type='lz4' 
)

topic_name = 'green-trips'
t0 = time.time()

# 4. Performance: Avoid iterrows() for large datasets
# itertuples() is significantly faster than iterrows()
for row in df.itertuples(index=False):
    # Map row namedtuple to your Ride model
    # Note: ensure ride_from_row can handle the object returned by itertuples
    ride = ride_from_row(row._asdict()) 
    producer.send(topic_name, value=ride)
    
    # 5. Remove time.sleep(0.01)
    # If you sleep 0.01s for 500k rows, the script will take 83+ minutes!
    # Let Kafka's internal buffering handle the flow.

producer.flush()
t1 = time.time()
print(f'Sent {len(df)} rows in {(t1 - t0):.2f} seconds')
