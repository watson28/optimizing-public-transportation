"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import os
import requests

logger = logging.getLogger(__name__)

CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    kafka_connect_url = f"{os.getenv('KAFKA_CONNECT_URL')}/connectors"
    resp = requests.get(f"{kafka_connect_url}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        kafka_connect_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": os.getenv('DB_CONNECTION_URL'),
                "connection.user": os.getenv('DB_USERNAME'),
                "connection.password": os.getenv('DB_PASSWORD'),
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "com.udacity.project.chicago_transportation.station.",
                "poll.interval.ms": "600000",
            }
        }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
