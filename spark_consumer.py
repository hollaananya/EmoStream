from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import time 
import math 

# Initialize Spark context and streaming context
sc = SparkContext(appName="EmojiAggregator")
#ssc = StreamingContext(sc, 2)  # Batch interval of 2 seconds

# Kafka topic and broker information
kafka_topic = "emoji_topic"
kafka_brokers = "localhost:9092"  # Modify if your Kafka broker is on a different server/port

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_brokers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def main_publisher(data):
    producer.send('main_topic', data)
    producer.flush()
    print(f"Data sent to main topic: {data}")

k = time.time()+2
ans = []
for i in consumer:
	ans.append(i.value['emoji'])
	if(time.time()>=k):
		emoji_rdd = sc.parallelize(ans)
		emoji_counts = emoji_rdd.countByValue()
		scaling_factor = emoji_rdd.count()/4
		for i in emoji_counts:
			emoji_counts[i] = math.ceil(emoji_counts[i]/scaling_factor)
		
		#most_common_emoji = max(emoji_counts, key=emoji_counts.get)
		
		#print(f"The most common emoji is: {most_common_emoji}")
		#print(len(ans))
		main_publisher(emoji_counts)
		
		k = time.time()+2 
		ans = []
'''
# Create a DStream from the Kafka consumer
kafka_stream = ssc.queueStream([consumer])
print("Kafka stream created")

# Parse the received data
parsed_stream = kafka_stream.map(lambda data: data)
print("Parsed stream data:", parsed_stream.collect())

parsed_stream.pprint()
# Extract and count the emojis
emoji_counts = parsed_stream.map(lambda data: (data["emoji"], 1)).reduceByKey(lambda a, b: a + b)

# Print the aggregated counts for each batch
emoji_counts.foreachRDD(lambda rdd: rdd.foreach(lambda record: print(f"Emoji: {record[0]}, Count: {record[1]}")))

# Start the Spark streaming context and await termination
ssc.start()
ssc.awaitTermination()'''
