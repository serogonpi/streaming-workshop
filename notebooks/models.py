import json
from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    passenger_count: int
    total_amount: float


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=row['lpep_pickup_datetime'],
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'],
        passenger_count=int(row['passenger_count']),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)