import json
from dataclasses import dataclass

@dataclass
class Ride:
    pulocationid : int
    dolocationid: int
    trip_distance: float
    total_amount: float
    lpep_pickup_datetime: str  # CHANGED: must be string for Flink
    lpep_dropoff_datetime: str # CHANGED: must be string for Flink
    passenger_count: int
    tip_amount: float

def ride_from_row(row):
    pickup = row.get('lpep_pickup_datetime')
    dropoff = row.get('lpep_dropoff_datetime')
    return Ride(
        pulocationid =int(row['pulocationid']),
        dolocationid=int(row['dolocationid']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        passenger_count=int(row['passenger_count']),
        tip_amount=float(row['tip_amount']),
        # CHANGED: Convert to string format 'yyyy-MM-dd HH:mm:ss'
        lpep_pickup_datetime=pickup.strftime('%Y-%m-%d %H:%M:%S') if hasattr(pickup, 'strftime') else str(pickup),
        lpep_dropoff_datetime=dropoff.strftime('%Y-%m-%d %H:%M:%S') if hasattr(dropoff, 'strftime') else str(dropoff)
    )

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
