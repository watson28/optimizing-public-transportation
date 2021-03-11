"""Producer base-class providing common utilites and functionality"""
import logging
import time
import os
import json

from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    admin: AdminClient = None

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": os.getenv('KAFKA_URL'),
            "schema.registry.url": os.getenv('SCHEMA_REGISTRY_URL')
        }

        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(self.broker_properties, key_schema, value_schema)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        admin = self._get_admin_client()
        topic_metadata: ClusterMetadata = admin.list_topics(timeout=5)
        topic_producer_exists: bool = self.topic_name in topic_metadata.topics

        if topic_producer_exists:
            return

        futures = admin.create_topics([
            NewTopic(
                topic = self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas,
                config={
                    "cleanup.policy": "delete",
                    "compression.type": "lz4",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                }
            )
        ])

        topic_creation = futures[self.topic_name]
        try:
            topic_creation.result()
        except Exception as exception:
            logger.error('Failed to create Kafka topic: %s', self.topic_name)
            raise exception

    def produce(self, value):
        """Produce a kafka event"""
        logger.info("producing event: %s", self.topic_name)
        try:
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value=value
            )
        except Exception as exception:
            logger.error(
                'Failed to send event to kafka.\nTopic name: %s\nEvent value: %s\n',
                self.topic_name,
                json.dumps(value)
            )
            raise exception

    def _get_admin_client(self):
        if Producer.admin is None:
            Producer.admin = AdminClient({ 'bootstrap.servers': os.getenv('KAFKA_URL')})
        return Producer.admin

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
