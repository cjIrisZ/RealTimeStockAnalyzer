from googlefinance import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

import argparse
import schedule
import logging
import datetime
import time
import json
import atexit

# default the topic name & kafka broker
topic_name = 'stock_analyzer'
kafka_broker = '127.0.0.1:9092'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

def fetch_price(producer, symbol):
    logger.debug('Start to fetch stock price for %s', symbol)

    try:
        price = json.dumps(getQuotes(symbol))
        logger.debug('Retrieved stock info %s', price)
        producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
        logger.debug('Sent stock price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error.message))
    except Exception:
        logger.warn('Failed to fetch stock price for %s', symbol)

def shutdown_hook(producer):
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)


if __name__ == '__main__':
    # setup command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='the symbol of the stock to collect')
    parser.add_argument('topic_name', help='the kafka topic push to')
    parser.add_argument('kafka_broker', help='the location of kafka broker')

    # parse args
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # a simple kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker
    )

    # schedule and run the fetch_price function every other second
    schedule.every(2).seconds.do(fetch_price, producer, symbol)

    # setup proper shutdonw hook
    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)
