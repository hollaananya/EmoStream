import requests
import logging
import time
import random
import threading
from kafka import KafkaConsumer
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of emojis
emojis = ["\U0001f600", "\U0001f602", "\U0001f60d", "\U0001f44d"]

class SubscriptionClient:
    def __init__(self, subscription_url="http://localhost:5002", emoji_url="http://localhost:5001"):
        self.subscription_url = subscription_url
        self.emoji_url = emoji_url
        self.client_id = None
        self.subscription_key = None
        self.emoji_thread = None
        self.receiver_thread = None
        self.running = False
        
    def receive_emoji(self, client_id):
        """Receive emojis from Kafka for a specific client ID"""
        kafka_topic = client_id
        kafka_brokers = "localhost:9092"
        
        try:
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_brokers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            for message in consumer:
                if not self.running:
                    break
                # Print the message value instead of trying to access index 6
                k= message.value
                s = ""
                for i in k:
                	s = s+i*k[i]
                logger.info(f"Received emoji: {s}")
           
                
        except Exception as e:
            logger.error(f"Error in Kafka consumer for {client_id}: {str(e)}")
    
    def start_emoji_receiving(self, client_id):
        """Start emoji receiving in a separate thread"""
        self.receiver_thread = threading.Thread(target=self.receive_emoji, args=(client_id,))
        self.receiver_thread.daemon = True
        self.receiver_thread.start()
        
    def send_emojis(self, rate):
        """Send emojis at specified rate"""
        self.running = True
        while self.running:
            try:
                emoji = random.choice(emojis)
                data = {
                    "emoji": emoji,
                    "client_id": self.client_id,
                    "timestamp": time.time()
                }
                # Send POST request to the emoji Flask API
                response = requests.post(
                    f"{self.emoji_url}/emoji",
                    json=data
                )
                '''if response.status_code == 200:
                    logger.info(f"Client {self.client_id} sent emoji: {emoji}")
                else:
                    logger.warning(f"Client {self.client_id} failed to send emoji. Status code: {response.status_code}")'''
                # Sleep for a random time between 0 and 2/rate seconds
                time.sleep(random.uniform(0, 2.0 / rate))
            except Exception as e:
                logger.error(f"Error in client {self.client_id}: {str(e)}")
                if not self.running:  # Break the loop if client is stopping
                    break
                time.sleep(1)  # Wait before retrying if error occurs
    
    def start_emoji_sending(self, rate):
        """Start emoji sending in a separate thread"""
        self.emoji_thread = threading.Thread(target=self.send_emojis, args=(rate,))
        self.emoji_thread.daemon = True  # Thread will stop when main program stops
        self.emoji_thread.start()
    
    def subscribe(self, client_id, cluster_id, subscriber_id):
        """Subscribe client to a specific subscription key"""
        self.client_id = client_id
        self.subscription_key = cluster_id + subscriber_id
        
        try:
            response = requests.post(
                f"{self.subscription_url}/subscribe",
                json={
                    "subscription_key": self.subscription_key,
                    "client_id": self.client_id
                }
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully subscribed client {self.client_id}")
                return response.json()
            else:
                logger.error(f"Failed to subscribe. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error in subscribe: {str(e)}")
            return None
    
    def unsubscribe(self):
        """Unsubscribe client and stop emoji sending"""
        if not self.client_id or not self.subscription_key:
            logger.error("Client not subscribed")
            return None
        
        # Stop emoji sending and receiving
        self.running = False
        if self.emoji_thread:
            self.emoji_thread.join(timeout=2)
        if self.receiver_thread:
            self.receiver_thread.join(timeout=2)
            
        try:
            response = requests.post(
                f"{self.subscription_url}/unsubscribe",
                json={
                    "subscription_key": self.subscription_key,
                    "client_id": self.client_id
                }
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully unsubscribed client {self.client_id}")
                self.client_id = None
                self.subscription_key = None
                return response.json()
            else:
                logger.error(f"Failed to unsubscribe. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error in unsubscribe: {str(e)}")
            return None
    
    def get_subscriptions(self):
        """Get current state of all subscriptions"""
        try:
            response = requests.get(f"{self.subscription_url}/subscriptions")
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get subscriptions. Status code: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error in get_subscriptions: {str(e)}")
            return None

def main():
    client = SubscriptionClient()
    
    # Get user input
    client_id = input("Enter client ID: ")
    cluster_id = input("Enter cluster ID: ")
    subscriber_id = input("Enter subscriber ID: ")
    emoji_rate = int(input("Enter emojis per second rate: "))
    
    # Subscribe the client
    result = client.subscribe(client_id, cluster_id, subscriber_id)
    if result:
        logger.info("Subscription successful!")
        logger.info(f"Current subscriptions: {result['subscriptions']}")
        
        # Start both sending and receiving emojis
        client.start_emoji_sending(emoji_rate)
        client.start_emoji_receiving(client_id)
    
    try:
        # Keep the main thread running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        # Unsubscribe when client is terminated
        client.unsubscribe()
        logger.info("Client terminated and unsubscribed")

if __name__ == "__main__":
    main()
