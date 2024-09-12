import os
import time 
from google.cloud import pubsub_v1


if __name__ == "__main__":

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "apache-beam-learning-435013-5bf563fc7165.json"

    subscription_path = "projects/apache-beam-learning-435013/subscriptions/Topic2-sub"

    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print('Received message : {}'.format(message))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)

