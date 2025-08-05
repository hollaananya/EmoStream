import sys
from kafka import KafkaConsumer
import json 
from kafka import KafkaProducer
import requests
import logging
import time

kafka_topic = 'cluster2'
kafka_brokers = "localhost:9092"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def monitor_subscriptions(subscription_url="http://localhost:5002"):
    """Monitor and print subscription updates from the server"""
    #logger.info("Starting subscription monitor...")
    
    
    try:
            response = requests.get(f"{subscription_url}/subscriptions")
            if response.status_code == 200:
                subscriptions = response.json()
                return subscriptions
                #logger.info(f"Current subscriptions: {subscriptions}")
            else:
                logger.error(f"Failed to get subscriptions. Status code: {response.status_code}")
        
    except Exception as e:
            logger.error(f"Error getting subscriptions: {str(e)}")
        
        #time.sleep(5)  # Check every 5 seconds
     
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
	print(i[6])
	data = i[6]
	sub = monitor_subscriptions()
	ans = []
	for i in sub:
		if(i[0]=="2"):
			ans +=sub[i]
	for j in ans:
		producer.send(j, data)
		
		
	
	
