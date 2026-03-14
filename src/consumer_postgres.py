import json
import psycopg2
from datetime import datetime

from models import Ride

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='latest',
    group_id='green-trips-postgres'
    )

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
for message in consumer:
    try:
        # Llamamos al deserializador manualmente aquí
        ride = ride_deserializer(message.value)
        cur.execute(
            
            """INSERT INTO processed_events
            (   lpep_pickup_datetime,
                lpep_dropoff_datetime,
                PULocationID,
                DOLocationID,
                passenger_count,
                trip_distance,
                tip_amount,
                total_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (ride.lpep_pickup_datetime, ride.lpep_dropoff_datetime,
            ride.PULocationID, ride.DOLocationID, ride.passenger_count,
            ride.trip_distance, ride.tip_amount, ride.total_amount)
        )
        count += 1
        if count % 100 == 0:
            print(f"Inserted {count} rows...")
    except Exception as e:
        print(f"⚠️ Saltando mensaje corrupto en offset {message.offset}: {e}")
        # Al no hacer 'raise', el bucle continúa al siguiente mensaje
        # y Kafka marcará este offset como "leído" (si auto_commit está on)
        continue

consumer.close()
cur.close()
conn.close()