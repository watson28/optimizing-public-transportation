"""Producer base-class providing common utilites and functionality"""
import logging
import time
import os
from collections.abc import Iterator


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata, TopicMetadata
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

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
        admin = AdminClient({ 'bootstrap.servers': os.getenv('KAFKA_URL')})
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
        except Exception as e:
            logger.error(f'Failed to create Kafka topic: {self.topic_name}')
            raise e

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
