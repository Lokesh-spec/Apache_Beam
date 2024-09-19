import os
import time
import random
import json
import argparse
import pandas as pd
import logging
from google.cloud import pubsub_v1
from google.auth.exceptions import GoogleAuthError

def generate_customer_data(customer_ids: list, pubsub_topic: str, project: str) -> None:
    try:
        publisher = pubsub_v1.PublisherClient()
    except GoogleAuthError as e:
        logging.error(f"Failed to create Pub/Sub client: {e}")
        return
    
    customer_data = []
    
    for _ in range(150):
        customer_id = random.choice(customer_ids)
        purchase_amount = round(random.uniform(10.00, 100.00), 2)
        time_stamp = time.time()

        event_data = {
            "customer_id": customer_id,
            "purchase_amount": purchase_amount,
            "time_stamp": time_stamp
        }

        event_data_str = json.dumps(event_data)
        event_data_bytes = event_data_str.encode('utf-8')

        try:
            logging.info(f"Publishing {event_data} to {pubsub_topic}")
            publisher.publish(pubsub_topic, event_data_bytes)
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")
            continue
        
        customer_data.append([customer_id, purchase_amount, time_stamp])
        time.sleep(1)

    df = pd.DataFrame(customer_data, columns=["Customer ID", "Purchase Amount", "Time Stamp"])
    df.to_csv("customer_data.csv", index=False)
    logging.info("Customer data saved to customer_data.csv")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate and publish customer data.')
    parser.add_argument('--project', type=str, required=True, help='Google Cloud Project ID')
    parser.add_argument('--pubsub_topic', type=str, required=True, help='Pub/Sub topic path')
    
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Generate a list of customer IDs
    customer_ids = [f"CO{'0' if i < 10 else ''}{i}" for i in range(100)]

    # Ensure GOOGLE_APPLICATION_CREDENTIALS is set
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        logging.info("Google credentials set correctly.")
        generate_customer_data(customer_ids, args.pubsub_topic, args.project)
    else:
        logging.error("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
