from kafka import KafkaConsumer, KafkaProducer
import json
import random

CONSUMER_TOPIC = 'order_detail'
PRODUCER_TOPIC = 'order_confirm'
BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'order_detail'

try:
    consumer = KafkaConsumer(CONSUMER_TOPIC,bootstrap_servers=BOOTSTRAP_SERVERS, group_id=GROUP_ID)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    print('Transaction backend starts listening')
    while 1:
        for msg in consumer:
            data = json.loads(msg.value.decode())
            print('Getting a new message')
            print(data)
            print('Processing data')
            data = {
                    'name': data['name'],
                    'email': data['email'],
                    'order_number': data['order_number'],
                    'price': random.randint(0,100)
                    }
            print('Sending processed message')
            print(data)
            ack = producer.send(PRODUCER_TOPIC, json.dumps(data).encode('utf-8'))
            print('ack {}'.format(ack))
            print('=======================================================')

except Exception as e:
    print('There is an error to register an consumer')
    print(e)
