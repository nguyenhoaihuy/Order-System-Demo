from kafka import KafkaProducer
import time
import json
from datetime import datetime


PRODUCER_TOPIC = 'order_detail'
BOOTSTRAP_SERVERS = 'localhost:9092'
INTERVAL = 0.001 # period of time between each send
NUMBER_OF_SEND = 10000

try:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    for i in range(NUMBER_OF_SEND):
        data = {
                   'name': 'Smith {}'.format(i),
                   'email': 'smith{}.gmail.com'.format(i),
                   'order_number': i,
                   'time': datetime.now().strftime("%H:%M:%S")
                }
        print(data)
        producer.send(PRODUCER_TOPIC, json.dumps(data).encode('utf-8'))
        time.sleep(INTERVAL)
except Exception as e:
    print('There is an error')
    print(e)


