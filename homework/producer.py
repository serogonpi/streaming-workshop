import json
import pandas as pd
import glob
import time

from kafka import KafkaProducer
from dataclasses import dataclass
from models import Ride, ride_from_row, ride_serializer

TOPIC_NAME = 'green-trips'
SERVER = 'localhost:9092'

parquet_file = r'/home/sebastian/projects/streaming-workshop/homework/green_tripdata_2025-10.parquet'
columns = ['lpep_pickup_datetime', 
           'lpep_dropoff_datetime',
           'PULocationID',
           'DOLocationID',
           'passenger_count',
           'trip_distance',
           'tip_amount',
           'total_amount']
df = pd.read_parquet(parquet_file, columns=columns)
df = df.where(df.notna(), None)

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=ride_serializer
)

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(TOPIC_NAME, value=ride)
    print(f"Sent: {ride}")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
