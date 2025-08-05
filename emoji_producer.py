from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import threading
import time
import logging
from queue import Queue

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask App Setup
app = Flask(__name__)

# Shared queue for emoji data
emoji_queue = Queue()

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    batch_size=16384,  # Batch size in bytes
    linger_ms=500,     # Flush interval (500ms)
    buffer_memory=33554432  # Buffer memory size
)

class KafkaProducerWorker(threading.Thread):
    def __init__(self, emoji_queue):
        super().__init__()
        self.queue = emoji_queue
        self.running = True

    def run(self):
        while self.running:
            try:
                # Get data from queue (if available)
                if not self.queue.empty():
                    data = self.queue.get()
                    # Send to Kafka topic
                    producer.send('emoji_topic', data)
                    logger.debug(f"Sent to Kafka: {data}")
                
                # Small sleep to prevent CPU overuse
                time.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Error in Kafka producer: {str(e)}")

@app.route('/emoji', methods=['POST'])
def receive_emoji():
    try:
        data = request.get_json()
        if "emoji" in data:
            # Add to queue for async processing
            emoji_queue.put(data)
            return jsonify({"message": "Emoji received"}), 200
        else:
            return jsonify({"error": "Invalid data"}), 400
    except Exception as e:
        logger.error(f"Error in API endpoint: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

def main():
    try:
        # Start Kafka producer worker
        kafka_worker = KafkaProducerWorker(emoji_queue)
        kafka_worker.start()
        
        # Start Flask app
        app.run(port=5001, debug=False, threaded=True)
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        # Clean shutdown
        kafka_worker.running = False
        producer.flush()
        producer.close()

if __name__ == '__main__':
    main()
