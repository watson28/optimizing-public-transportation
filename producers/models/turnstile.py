"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile():
    _producer: Producer = None

    def __init__(self, station):
        """Create the Turnstile"""
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)
        Turnstile._init_producer_singleton()

    @classmethod
    def _init_producer_singleton(cls):
        if cls._producer is None:
            key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
            value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")
            cls._producer = Producer(
                'com.udacity.project.chicago_transportation.station.turstile_entries',
                key_schema=key_schema,
                value_schema=value_schema
            )

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        for _ in range(num_entries):
            Turnstile._producer.produce({
                "station_id": self.station.station_id,
                "station_name": self.station.name,
                "line": self.station.color.name,
            })

    def close(self):
        Turnstile._producer.close()
