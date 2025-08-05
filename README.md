# EmoStream - Real-Time Emotion Analysis Pipeline

A distributed real-time emotion analysis system that processes emoji streams using Apache Kafka and Apache Spark to reveal audience sentiment trends and patterns.

## Overview

EmoStream demonstrates scalable real-time data processing by capturing emoji streams from multiple clients, aggregating them using Spark, and distributing processed results through a cluster-based subscription system. The system provides insights into collective emotional states and sentiment trends in real-time.

## What it does

- **Real-Time Emoji Streaming**: Captures emoji data from multiple clients simultaneously
- **Distributed Processing**: Uses Apache Spark for scalable emoji aggregation and analysis
- **Cluster-Based Distribution**: Routes processed data through subscription-based clusters
- **Sentiment Aggregation**: Counts and scales emoji frequencies to reveal emotion trends
- **Dynamic Subscriptions**: Allows clients to subscribe/unsubscribe to specific emotion clusters

## Architecture

### Data Flow Pipeline
1. **Emoji Clients** send emojis to Flask API endpoint
2. **Kafka Producer** publishes emoji streams to `emoji_topic`
3. **Spark Consumer** aggregates emoji counts in 2-second windows
4. **Cluster Distribution** routes aggregated data to cluster-specific topics
5. **Subscribers** filter and forward data to subscribed clients

### Core Components

**Emoji Producer (`emoji_producer.py`)**
- Flask API for receiving emoji data from clients
- Asynchronous Kafka producer with batching and buffering
- Queue-based processing for high throughput

**Spark Consumer (`spark_consumer.py`)**
- Real-time emoji aggregation using Apache Spark
- 2-second sliding windows for sentiment analysis
- Scaling factor calculation based on total emoji volume

**Cluster Manager (`cluster1.py`)**
- Distributes aggregated data across multiple clusters
- Publishes to cluster-specific Kafka topics

**Subscription System (`sub_server.py`)**
- RESTful API for client subscription management
- Dynamic subscription/unsubscription capabilities

**Cluster Subscribers (`subscriber1.py`, `subscriber2.py`, `subscriber3.py`)**
- Filter data based on cluster membership
- Route personalized emoji streams to individual clients

**Emoji Client (`emoji_client3.py`)**
- Multi-threaded client for sending and receiving emojis
- Configurable emoji transmission rates
- Real-time emoji consumption from personalized topics

## Tech Stack

**Stream Processing**
- Apache Kafka for distributed message streaming
- Apache Spark for real-time data aggregation
- PySpark for Python-based Spark applications

**Web Framework**
- Flask for RESTful API endpoints
- Threading for concurrent client handling

**Data Format**
- JSON for message serialization
- Kafka topics for data distribution

**Client Libraries**
- kafka-python for Kafka integration
- requests for HTTP communication

## How to Run

### Prerequisites
- Apache Kafka (running on localhost:9092)
- Apache Spark
- Python 3.7+
- Required packages: `kafka-python`, `pyspark`, `flask`, `requests`

### Installation
```bash
# Install required packages
pip install kafka-python pyspark flask requests

# Start Kafka server
# Ensure Kafka is running on localhost:9092
```

### Running the System

1. **Start the Subscription Server**
```bash
python sub_server.py
# Runs on port 5002
```

2. **Start the Emoji Producer**
```bash
python emoji_producer.py
# Runs on port 5001
```

3. **Start the Spark Consumer**
```bash
python spark_consumer.py
# Processes emoji streams and publishes aggregated data
```

4. **Start the Cluster Manager**
```bash
python cluster1.py
# Distributes data to cluster topics
```

5. **Start Cluster Subscribers**
```bash
# In separate terminals:
python subscriber1.py  # Handles cluster 1
python subscriber2.py  # Handles cluster 2
python subscriber3.py  # Handles cluster 3
```

6. **Run Emoji Clients**
```bash
python emoji_client3.py
# Follow prompts to enter:
# - Client ID
# - Cluster ID (1, 2, or 3)
# - Subscriber ID
# - Emoji transmission rate
```

### Usage Example
```bash
# Terminal 1: Start subscription server
python sub_server.py

# Terminal 2: Start emoji producer
python emoji_producer.py

# Terminal 3: Start Spark consumer
python spark_consumer.py

# Terminal 4: Start cluster manager
python cluster1.py

# Terminal 5-7: Start subscribers
python subscriber1.py
python subscriber2.py
python subscriber3.py

# Terminal 8+: Start clients
python emoji_client3.py
# Enter: client_id=user1, cluster_id=1, subscriber_id=sub1, rate=5
```

### System Monitoring
- Check subscription status: `GET http://localhost:5002/subscriptions`
- Monitor Kafka topics for message flow
- Observe emoji aggregation in Spark consumer logs
- Track client emoji streams in real-time

## Key Features

- Handles multiple concurrent emoji streams
- Real-time sentiment aggregation with configurable windows
- Dynamic client subscription management
- Scalable cluster-based data distribution
- Fault-tolerant message processing
- Configurable emoji transmission rates
- Real-time emotion trend analysis
