from kafka import KafkaConsumer, KafkaProducer
import json
import random

CONSUMER_TOPIC = 'order_confirm'
BOOTSTRAP_SERVERS = 'localhost:9092'


try:
    consumer = KafkaConsumer(CONSUMER_TOPIC,bootstrap_servers=BOOTSTRAP_SERVERS)
    print('Analyer backend starts listening')
    while 1:
        for msg in consumer:
            data = json.loads(msg.value.decode())
            print('Getting a message')
            print(data)

            print('Analyzing the order number {}'.format(data['order_number']))
            print('=========================================================')

except Exception as e:
    print('There is an error')
    print(e)

