import json
import math
from dataclasses import dataclass
import dataclasses

@dataclass
class Ride:
    lpep_pickup_datetime: str  # epoch milliseconds
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float

# Recibe fila y devuelve un Ride, convirtiendo los tipos de datos según corresponda
def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f'),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S.%f'),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )

# Convierte un Ride a JSON bytes para enviar a Kafka
def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_dict = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in ride_dict.items()}
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

# Convierte JSON bytes de Kafka a un Ride
def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)