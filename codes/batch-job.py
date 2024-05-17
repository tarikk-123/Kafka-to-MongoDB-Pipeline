from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import pandas as pd

# Create a Kafka Consumer
consumer = KafkaConsumer('search-engine', bootstrap_servers="192.168.0.56:9092", auto_offset_reset="earliest")

# Create a MongoDB connection
mongo_client = MongoClient('mongodb://192.168.0.56:27017/')

# Select the database and collection
db = mongo_client.mydb
collection = db.analytic_data

# Create a list to store messages from Kafka
messages = []

# Read each message from Kafka and add it to the list
for msg in consumer:
    # Convert the message from Kafka to JSON format
    msg_dict = json.loads(msg.value.decode('utf-8'))
    
    # Add the message to the list
    messages.append(msg_dict)

    # When the desired number of messages is reached, process them
    if len(messages) >= 300:
        # Convert messages to a DataFrame
        df = pd.DataFrame(messages)

        # Print the DataFrame and its columns (for debugging purposes)
        print(df)
        print(df.columns)

        # Count occurrences of each search term
        search_counts = df.groupby('search').size().reset_index(name='counts')

        # Print the results (for debugging purposes)
        print(search_counts)

        # Send the results to the MongoDB collection
        collection.insert_many(search_counts.to_dict('records'))

        # Exit the loop
        break

    else:
        continue