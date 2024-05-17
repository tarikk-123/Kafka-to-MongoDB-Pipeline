import json
import random
import time
from kafka import KafkaProducer

def generate_search_data(users,search_terms):

    # Pick a random user
    user = random.choice(users)
    
    # Pick a random search term
    search = random.choice(search_terms)
    
    # Capture the current time
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
  # Create json data 
    data = {
        "user": user,
        "search": search,
        "timestamp": timestamp
        }

    return data    
    
def send_data_to_kafka(sample_data):
             
             bootstrap_servers = '192.168.0.56:9092'

             producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
              
             # Kafka topic
             topic = "search-engine"

             # Convert the data to JSON string              
             message = json.dumps(sample_data)
             
             # Send data to kafka topic
             producer.send(topic, value=message.encode('utf-8'))


if __name__ == "__main__":

    # 20 users
    users = ["John", "Emma", "Michael", "Sophia", "Daniel", "Olivia", "James", "Ava", "William", "Isabella", "Alexander", "Mia", "Ethan", "Charlotte", "Benjamin", "Amelia", "Jacob", "Harper", "Mason", "Evelyn"]

    # 30 most searched words
    search_terms = ["Weather","News","Bitcoin","Football","Recipes","Movies","Stocks","Travel","Jobs","Fashion","Gaming","Fitness","Music","Books","Health","Technology","Education","Food","Pets","Art","Home decor","DIY","Photography","Celebrity gossip","Home workouts","Streaming services","Virtual events","Online courses","Eco-friendly products","Local businesses"]

    while True:

        # Generate sample search data
        sample_data = generate_search_data(users, search_terms)

        # Send the data to Kafka
        send_data_to_kafka(sample_data)
        
        # Wait for 4 seconds before sending the next data        
        time.sleep(4)