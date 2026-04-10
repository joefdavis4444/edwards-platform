import pandas as pd
import random
import os
from faker import Faker
from datetime import date, timedelta

fake = Faker()

Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../../storage/raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_manufacturers(_n=10):
    records = []
    for i in range(1, _n+1):
        records.append({
            'manufacturer_id': f'MFG{i:03d}',
            'manufacturer_name': fake.company(),
            'city': fake.city(),
            'country': fake.country(),
            'region': random.choice(['North America', 'Europe', 'Asia Pacific', 'Latin America']),
            'fda_approved': random.choice([True, False]),
            'certification_status': random.choice(['ISO 13485', 'CE Marked', 'Pending', 'Suspended'])
        })
    return pd.DataFrame(records)

def generate_devices(manufacturers_df, n=50):

    MODEL_TYPE_MAP={
        'HV-2000': 'Heart Valve',
        'HV-3000': 'Heart Valve',
        'HM-100': 'Hemodynamic Monitor',
        'HM-200': 'Hemodynamic Monitor',
        'DP-500': 'Pressure Sensor'
    }

    records = []
    for i in range(1, n+1):
        model = random.choice(list(MODEL_TYPE_MAP.keys()))
        records.append({
            'device_id': f'DEV{i:04d}',
            'device_model': model,
            'device_type': MODEL_TYPE_MAP[model],
            'firmware_version': random.choice(['v1.0', 'v1.1', 'v1.2', 'v1.3', 'v1.4']),
            'status': random.choice(['Active', 'Recalled', 'Under Review', 'Retired']),
            'manufacturer_id': random.choice(manufacturers_df['manufacturer_id'].tolist())
        })
    return pd.DataFrame(records)    


manufacturers = generate_manufacturers()
devices = generate_devices(manufacturers)
print(devices)