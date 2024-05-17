from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Create a Kafka Consumer
consumer = KafkaConsumer('search-engine', bootstrap_servers="192.168.0.56:9092", auto_offset_reset="earliest")

# Create a MongoDB connection
mongo_client = MongoClient('mongodb://192.168.0.56:27017/')

# Select the database and collection
db = mongo_client.mydb
collection = db.all_data

# Iterate over each message from Kafka and write it to MongoDB
for msg in consumer:

    # Decode the message from Kafka and convert it to a Python dictionary
    msg_dict = json.loads(msg.value.decode('utf-8'))

    # Print the message for debugging purposes
    print(msg_dict)

    # Insert the message into the MongoDB collection
    collection.insert_one(msg_dict)