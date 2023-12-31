from kafka import KafkaProducer
from generator import generate_fake_customers
import time
import json
import datetime

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return super().default(obj)
    


def produce_data():
    producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, cls=DateEncoder).encode('utf-8')
    )
    topic_name = 'test'
    list_messages = generate_fake_customers(10)
    for message in list_messages :
        print(message)
        producer.send(topic_name, value=message)
        producer.send(topic_name, value="  ")
        producer.flush()
        print()
        time.sleep(1)
    producer.close()