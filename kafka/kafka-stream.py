#!/usr/local/bin/python3.6
# -*- coding:utf-8 -*-

"""
Subject:
Date:
Version:
Description:
"""
import sys, os

import msgpack as msgpack
import random
import string

root_path = os.path.abspath("..")
sys.path.append('/'.join([root_path, 'bin']))
from CommonModules import *
# from CommonModules import ConnectionBuilder as cnb
from kafka import *
from kafka.errors import KafkaError


def func():
    """
    Description:
    
    """
    pass


def kproducer():
    """

    :return:
    """
    producer = KafkaProducer(bootstrap_servers=['hadoop102m:9092', 'hadoop103m:9092', 'hadoop104m:9092'],
                             # value_serializer=msgpack.dumps
                             value_serializer=lambda m: json.dumps(m).encode('ascii')
                            )

    # Asynchronous by default
    # future = producer.send('sample01', b'raw_bytes')

    # producer = KafkaProducer(value_serializer=msgpack.dumps)
    # letters = string.ascii_letters
    letters = string.printable
    for i in range(10):
        # producer.send('sample01', b'raw_bytes__' + bytes(i))
        producer.send('sample01', {'index': str(i)})
        # produce keyed messages to enable hashed part itioning
        producer.send('sample01', key=bytes(i), value='msg_' + str(i))
        # producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send('sample01', {'key': 'SN' + str(i) + ':' + ''.join(random.choice(letters) for i in range(10))})
        t.sleep(1)

    # # Block for 'synchronous' sends
    # try:
    #     record_metadata = future.get(timeout=10)
    # except KafkaError:
    #     # Decide what to do if produce request failed...
    #     logger.exception()
    #     pass
    #
    # # Successful result returns assigned partition and offset
    # print(record_metadata.topic)
    # print(record_metadata.partition)
    # print(record_metadata.offset)
    #
    # # produce keyed messages to enable hashed partitioning
    # producer.send('sample01', key=b'foo', value=b'bar')
    #
    # # encode objects via msgpack
    # producer = KafkaProducer(value_serializer=msgpack.dumps)
    # producer.send('sample01', {'key': 'value'})
    #
    # # produce json messages
    # producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # producer.send('sample01', {'key': 'value'})
    #
    # # produce asynchronously
    # for _ in range(100):
    #     producer.send('sample01', b'msg')
    #
    # def on_send_success(record_metadata):
    #     print(record_metadata.topic)
    #     print(record_metadata.partition)
    #     print(record_metadata.offset)
    #
    # def on_send_error(excp):
    #     logger.error('I am an errback', exc_info=excp)
    #     # handle exception
    #
    # # produce asynchronously with callbacks
    # producer.send('sample01', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)
    #
    # # block until all async messages are sent
    # producer.flush()
    #
    # # configure multiple retries
    # producer = KafkaProducer(retries=5)


def kcomsumer():

    # # consume earliest available messages, don't commit offsets
    # KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('sample01',
                             group_id='my-group',
                             bootstrap_servers=['hadoop102m'],
                             auto_offset_reset='earliest', enable_auto_commit=False)
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

    # # consume earliest available messages, don't commit offsets
    # KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)
    #
    # # consume json messages
    # KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
    #
    # # consume msgpack
    # KafkaConsumer(value_deserializer=msgpack.unpackb)
    #
    # # StopIteration if no message after 1sec
    # KafkaConsumer(consumer_timeout_ms=1000)

    # # Subscribe to a regex topic pattern
    # consumer = KafkaConsumer()
    # consumer.subscribe(pattern='^awesome.*')

    # # Use multiple consumers in parallel w/ 0.9 kafka brokers
    # # typically you would run each on a different server / process / CPU
    # consumer1 = KafkaConsumer('sample01',
    #                           group_id='my-group',
    #                           bootstrap_servers='my.server.com')
    # consumer2 = KafkaConsumer('my-topic',
    #                           group_id='my-group',
    #                           bootstrap_servers='my.server.com')


if __name__ == '__main__':
    """
    use logger instance to record log: info, error, warning, critical and debug
    send error, warning and critical messsage to slack at then end of the job 
    """
    # default header
    logger.info(f"Start of the job <{sys.argv[0]}>")

    # Slack token name and channel of this project
    # slack_token = ''  # e.g.'SLACK_BOT_TOKEN_CRM'
    # slack_channel = ''  # e.g. '#crm_notification'

    # add code here:
    if len(sys.argv) < 2:
        logger.error('invalid argv ..')
    else:
        if sys.argv[1] == 'p':
            kproducer()
        elif sys.argv[1] == 'c':
            kcomsumer()
        else:
            logger.error('invalid argv ..')

    # end
    logger.info(f"End of the job <{sys.argv[0]}>")
    # LoggingToSlack.slack_logging(slack_token, slack_channel, jobname=sys.argv[0])
