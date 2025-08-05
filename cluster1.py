import sys
from kafka import KafkaConsumer
import json 
from kafka import KafkaProducer

kafka_topic = 'main_topic'
kafka_brokers = "localhost:9092"
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_brokers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for i in consumer:
	data = i[6]
	print(data)
	producer.send('cluster1', data)
	producer.send('cluster2', data)
	producer.send('cluster3', data)
	producer.flush()
