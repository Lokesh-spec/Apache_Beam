import os
import time 
from google.cloud import pubsub_v1

if __name__ == "__main__":

    project = "apache-beam-learning-435013"

    pubsub_topic = "projects/apache-beam-learning-435013/topics/Topic1"

    path_service_account = "apache-beam-learning-435013-5bf563fc7165.json"

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_service_account  

    input_file = 'Data\store_sales.csv'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)    