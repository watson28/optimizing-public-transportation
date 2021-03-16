"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer
from models.train import Train


logger = logging.getLogger(__name__)


class Station():
    """Defines a single station"""
    _producer: Producer = None

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)
        Station._init_producer_singleton()

    @classmethod
    def _init_producer_singleton(cls):
        if cls._producer is None:
            key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
            value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")
            cls._producer = Producer(
                'com.udacity.project.chicago_transportation.arrival',
                key_schema=key_schema,
                value_schema=value_schema
            )

    def run(self, train: Train, direction: str, prev_station_id: int, prev_direction: str):
        """Simulates train arrivals at this station"""

        Station._producer.produce({
            "station_id": self.station_id,
            "train_id": train.train_id,
            "direction": direction,
            "line": self.color.name,
            "train_status": train.status.name,
            "prev_station_id": prev_station_id,
            "prev_direction": prev_direction
        })

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        Station._producer.close()
