import atexit
import logging
import json
import argparse
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

topic = 'stock-analyzer'
target_topic = 'average-price'
kafka_brokers = '127.0.0.1:9092'
kafka_producer = None


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


def process_stream(stream):

    def send_to_kafka(rdd):
        results = rdd.collect()
        for r in results:
            data = json.dumps(
                {
                    'symbol': r[0],
                    'timestamp': time.time(),
                    'average': r[1]
                }
            )
            try:
                logger.info('Sending average price %s to kafka' % data)
                kafka_producer.send(target_topic, value=data)
            except KafkaError as error:
                logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

    def pair(data):
        record = json.loads(data[1].decode('utf-8'))[0]
        return record.get('StockSymbol'), (float(record.get('LastTradePrice')), 1)

    stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)

if __name__ == '__main__':
    # setup command line args
    parser = argparse.ArgumentParser()
    parser.add_argument('topic', help='the kafka topic name')
    parser.add_argument('target_topic', help='topic name of the target')
    parser.add_argument('kafka_brokers', help='the location of kafka broker')

    # parse args
    args = parser.parse_args()
    topic = args.topic
    target_topic = args.target_topic
    kafka_brokers = args.kafka_brokers

    # create SparkContext and StreamingContext
    sc = SparkContext("local[2]", "StockAveragePrice")
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 5)

    # instantiate a kafka stream for processing
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_brokers})
    process_stream(directKafkaStream)

    # instantiate a simple kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_brokers
    )

    # setup proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
