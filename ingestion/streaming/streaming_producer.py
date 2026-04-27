import json
import time
import random
import pandas as pd
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from google.cloud import pubsub_v1
import os

fake = Faker()
Faker.seed(42)
random.seed(42)

BOOTSTRAP_SERVERS = os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS', 'localhost:9092')
CONFLUENT_API_KEY = os.environ.get('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.environ.get('CONFLUENT_API_SECRET')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=CONFLUENT_API_KEY,
    sasl_plain_password=CONFLUENT_API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

publisher = pubsub_v1.PublisherClient()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, '../../storage/raw')

devices = pd.read_csv(os.path.join(RAW_DIR, 'dim_device.csv'))
patients = pd.read_csv(os.path.join(RAW_DIR, 'dim_patient.csv'))
trial_sites = pd.read_csv(os.path.join(RAW_DIR, 'dim_trial_site.csv'))

device_ids = devices['device_id'].tolist()
patient_ids = patients['patient_id'].tolist()
trial_site_ids = trial_sites['trial_site_id'].tolist()

READING_TYPES = ['Scheduled', 'Alert', 'Manual']

EVENT_TYPES = ['battery low', 'anomaly detected', 'device offline', 'red alert' ]

def generate_telemetry_event():
    return {
        'reading_id': f"RDG{fake.numerify(text='#######')}",
        'device_id': random.choice(device_ids),
        'patient_id': random.choice(patient_ids),
        'trial_site_id': random.choice(trial_site_ids),
        'reading_type': random.choice(READING_TYPES),
        'heart_rate': round(random.uniform(45, 130), 1),
        'systolic_pressure': round(random.uniform(90, 180), 1),
        'diastolic_pressure': round(random.uniform(60, 120), 1),
        'pressure_gradient': round(random.uniform(0, 50), 2),
        'battery_level': round(random.uniform(0, 100), 1),
        'signal_strength': round(random.uniform(0, 100), 1),
        'event_ts': datetime.now(timezone.utc).isoformat(),
        '_ingestion_ts': datetime.now(timezone.utc).isoformat(),
        '_source_topic': 'device-telemetry-raw',
        '_producer_version': '1.0.0'
    }


def generate_device_event():
    return {
        'event_id': f"EVT{fake.numerify(text='#########')}",
        'device_id': random.choice(device_ids),
        'patient_id': random.choice(patient_ids),
        'trial_site_id': random.choice(trial_site_ids),
        'event_type': random.choice(EVENT_TYPES),
        'severity': random.choice(['low', 'medium', 'high']),
        'description': fake.sentence(),
        'event_ts': datetime.now(timezone.utc).isoformat(),
        '_source_topic': 'device-events-raw'
    }

while True:
    device_event = generate_device_event()
    telemetry_event = generate_telemetry_event()

    producer.send(
        topic='device-telemetry-raw',
        key=telemetry_event['reading_id'],
        value=telemetry_event
    )
    if random.randint(1, 10) == 1:
        topic_path = publisher.topic_path(PROJECT_ID, 'device-events-raw')
        publisher.publish(
            topic_path,
            data=json.dumps(device_event).encode('utf-8')
        )
        print(f"Event: {device_event['event_id']} | Type: {device_event['event_type']} | Severity: {device_event['severity']}")

    producer.flush()
   
    print(f"Telemetry: {telemetry_event['reading_id']} | Device: {telemetry_event['device_id']} | HR: {telemetry_event['heart_rate']}")
    

    time.sleep(1)


