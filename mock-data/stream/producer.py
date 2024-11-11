import os
import base64
import http.server
import datetime
import time
import random
import json
import logging
import google.auth
import google.auth.transport.urllib3
import urllib3
from faker import Faker
import pandas as pd
import confluent_kafka
import functools
from dotenv import load_dotenv

# Token Provider class
# This class handles the OAuth token retrieval and formatting
class TokenProvider(object):

  def __init__(self, **config):
    self.credentials, _project = google.auth.default()
    self.http_client = urllib3.PoolManager()
    self.HEADER = json.dumps(dict(typ='JWT', alg='GOOG_OAUTH2_TOKEN'))

  def valid_credentials(self):
    if not self.credentials.valid:
      self.credentials.refresh(google.auth.transport.urllib3.Request(self.http_client))
    return self.credentials

  def get_jwt(self, creds):
    return json.dumps(
        dict(
            exp=creds.expiry.timestamp(),
            iss='Google',
            iat=datetime.datetime.now(datetime.timezone.utc).timestamp(),
            scope='kafka',
            sub=creds.service_account_email,
        )
    )

  def b64_encode(self, source):
    return (
        base64.urlsafe_b64encode(source.encode('utf-8'))
        .decode('utf-8')
        .rstrip('=')
    )

  def get_kafka_access_token(self, creds):
    return '.'.join([
      self.b64_encode(self.HEADER),
      self.b64_encode(self.get_jwt(creds)),
      self.b64_encode(creds.token)
    ])

  def token(self):
    creds = self.valid_credentials()
    return self.get_kafka_access_token(creds)

  def confluent_token(self):
    creds = self.valid_credentials()

    utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
    expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()

    return self.get_kafka_access_token(creds), time.time() + expiry_seconds

# Confluent does not use a TokenProvider object
# It calls a method
def make_token(args):
  """Method to get the Token"""
  t = TokenProvider()
  token = t.confluent_token()
  return token

def delivery_callback(err, msg):
    """Delivery callback for producer."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def read_csv_file(file_path):
    """Read a CSV file and return a DataFrame."""
    return pd.read_csv(file_path)

def main():
    # Load environment variables from the .env file in the same directory
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

    # Read environment variables
    kafka_cluster_name = os.getenv("KAFKA_CLUSTER_NAME")
    region = os.getenv("REGION", "us-central1")
    project_id = os.getenv("PROJECT_ID")
    port = '9092'
    kafka_topic_name = os.getenv("CONFLUENT_TOPIC", "heartrate-topic1")

    # Configure the Kafka producer
    producer_config = {
        "bootstrap.servers": f"bootstrap.{kafka_cluster_name}.{region}.managedkafka.{project_id}.cloud.goog:{port}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": make_token,
        "request.timeout.ms": 250000,
        "socket.timeout.ms": 250000,  # Increase request timeout
        "linger.ms": 10,                # Enable batching
        "acks": "all",                 # Wait for all replicas to acknowledge
    }
    
    producer = confluent_kafka.Producer(producer_config)

    # Load data from CSV files
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "static", "data"))
    users_df = read_csv_file(os.path.join(base_dir, "users.csv"))
    activities_df = read_csv_file(os.path.join(base_dir, "activities.csv"))

    fake = Faker()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        while True:
            # Randomly select a user and activity
            user_id = random.choice(users_df["userid"])
            user_country = users_df.loc[users_df["userid"] == user_id, "address_country_code"].iloc[0]
            activity_id = random.choice(activities_df["activity_id"])

            # Get the activity name (for heart rate simulation)
            activity_name = activities_df.loc[activities_df["activity_id"] == activity_id, "activity_name"].iloc[0]

            # Generate latitude and longitude based on user country
            latlng = fake.local_latlng(country_code=user_country)

            # Generate heart rate based on activity type
            if activity_name in ["Stand", "Sit", "Sleep"]:
                heart_rate = random.randint(40, 80)
            elif activity_name in ["Run", "HIIT", "Swimming", "Kickboxing", "Multisport"]:
                heart_rate = random.randint(130, 205)
            else:
                heart_rate = random.randint(70, 150)

            # Create the message payload
            hr_data = {
                "user_id": int(user_id),  # Ensure this is a standard int
                "heart_rate": heart_rate,
                "timestamp": int(time.time()),
                "meta": {
                    "activity_id": int(activity_id),  # Ensure this is a standard int
                    "location": {
                        "latitude": float(latlng[0]),
                        "longitude": float(latlng[1])
                    }
                }
            }

            # Produce the message to the Kafka topic
            producer.produce(
                kafka_topic_name,
                key=str(user_id).encode("utf-8"),
                value=json.dumps(hr_data).encode("utf-8"),
                callback=delivery_callback,
            )

            logging.info(f"Produced message: {json.dumps(hr_data, indent=2)}")

            # Flush the producer to ensure all messages are sent
            producer.flush()

            # Send a new message every 2 seconds
            time.sleep(2)

    except KeyboardInterrupt:
        logging.info("Stream stopped successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        producer.flush()  # Ensure any remaining messages are sent before exiting


if __name__ == "__main__":
    main()
