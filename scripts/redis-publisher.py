# - read from kafka topic
# - publish to redis pub

from kafka import KafkaConsumer

import redis
import logging
import atexit
import argparse

logging.basicConfig()
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.INFO)

if __name__ == '__main__':

    # setup command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name')
    parser.add_argument('kafka_broker')
    parser.add_argument('redis_channel')
    parser.add_argument('redis_host')
    parser.add_argument('redis_port')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    redis_channel = args.redis_channel
    redis_host = args.redis_host
    redis_port = args.redis_port

    # kafka consumer
    kafka_consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )

    # redis client
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

    for msg in kafka_consumer:
        logger.info('received new data from kafka %s' % msg.value)
        redis_client.publish(redis_channel, msg.value)
