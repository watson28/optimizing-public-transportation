"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import os
from dotenv import load_dotenv, find_dotenv
import requests

import topic_check


logger = logging.getLogger(__name__)
load_dotenv(find_dotenv())



KSQL_URL = os.getenv('KSQL_URL')

KSQL_STATEMENT = """
CREATE TABLE turnstile
  (station_id INT,
   station_name VARCHAR,
   line VARCHAR
  )
  WITH (KAFKA_TOPIC='com.udacity.project.chicago_transportation.station.turstile_entries',
        KEY='station_id',
        VALUE_FORMAT='AVRO'
  );

CREATE TABLE turnstile_summary
  WITH (VALUE_FORMAT='JSON') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        logger.info('TURNSTILE_SUMMARY Table already exists. skipping table creation.')
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
