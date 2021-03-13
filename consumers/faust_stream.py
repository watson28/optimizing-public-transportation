"""Defines trends calculations for stations"""
import logging
import os
from dataclasses import dataclass

import faust
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
logger = logging.getLogger(__name__)

@dataclass
class Station(faust.Record):
    """Format for received events"""
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool

@dataclass
class TransformedStation(faust.Record):
    """Format for produced events"""
    station_id: int
    station_name: str
    order: int
    line: str

broker = os.getenv('KAFKA_URL').replace('PLAINTEXT', 'kafka')
app = faust.App("stations-stream", broker=broker, store="memory://")
station_topic = app.topic(
    'com.udacity.project.chicago_transportation.station.stations',
    key_type=None,
    value_type=Station
)
transformed_station_topic = app.topic(
    'com.udacity.project.chicago_transportation.station.transformed',
    value_type=TransformedStation,
    partitions=1
)

table = app.Table(
    'stations',
    default=TransformedStation,
    partitions=1,
    changelog_topic=transformed_station_topic,
)

@app.agent(station_topic)
async def transform_stations(stations):
    async for station in stations:
        transformed = TransformedStation(
            station_id= station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=get_station_color(station)
        )
        await transformed_station_topic.send(value=transformed)

def get_station_color(station: Station):
    if station.red:
        return 'red'
    if station.blue:
        return 'blue'
    if station.green:
        return 'green'
    return None

if __name__ == "__main__":
    app.main()
