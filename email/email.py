from kafka import KafkaConsumer, KafkaProducer
import json
import random

CONSUMER_TOPIC = 'order_confirm'
BOOTSTRAP_SERVERS = 'localhost:9092'


try:
    consumer = KafkaConsumer(CONSUMER_TOPIC,bootstrap_servers=BOOTSTRAP_SERVERS)
    print('Email sender starts listening')
    while 1:
        for msg in consumer:
            data = json.loads(msg.value.decode())
            print('Getting a message')
            print(data)

            print('Sent email to {}'.format(data['email']))
            print('=========================================================')

except Exception as e:
    print('There is an error')
    print(e)
