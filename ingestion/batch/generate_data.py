import pandas as pd
import random
import os
from faker import Faker
from datetime import date, timedelta

fake = Faker()

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

df = generate_manufacturers()
print(df)