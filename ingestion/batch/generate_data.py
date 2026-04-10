import pandas as pd
import random
import os
import holidays
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


REGION_MAP = {
    'North America': [('New York', 'USA'), ('Toronto', 'Canada'), ('Mexico City', 'Mexico'), ('Chicago', 'USA'), ('Los Angeles', 'USA')],
    'Europe': [('London', 'UK'), ('Berlin', 'Germany'), ('Paris', 'France'), ('Amsterdam', 'Netherlands'), ('Zurich', 'Switzerland')],
    'Asia Pacific': [('Tokyo', 'Japan'), ('Sydney', 'Australia'), ('Singapore', 'Singapore'), ('Seoul', 'South Korea'), ('Mumbai', 'India')],
    'Latin America': [('São Paulo', 'Brazil'), ('Buenos Aires', 'Argentina'), ('Bogotá', 'Colombia'), ('Lima', 'Peru'), ('Santiago', 'Chile')]
}

def generate_trial_sites(n=20):
    records = []
    for i in range(1, n+1):
        region = random.choice(list(REGION_MAP.keys()))
        city, country = random.choice(REGION_MAP[region])
        records.append({
            'trial_site_id': f'SITE{i:03d}',
            'site_name': f'{city} Clinical Research Center',
            'city': city,
            'country': country,
            'region': region,
            'status': random.choice(['Active', 'On Hold', 'Closed'])
        })
    return pd.DataFrame(records)

def generate_patients(trial_sites, n=200):
    records = []
    for i in range(1, n+1):
        records.append({
            'patient_id': f'PATIENT{i:04d}',
            'trial_site_id': random.choice(trial_sites['trial_site_id'].tolist()),
            'diagnosis': random.choice(['Aortic Stenosis', 'Mitral Regurgitation', 'Heart Failure', 'Atrial Fibrillation', 'Coronary Artery Disease']),
            'severity_level': random.choice(['Mild', 'Moderate', 'Severe', 'Critical']),
            'treatment': random.choice(['HV-2000 Implant', 'HV-3000 Implant', 'Hemodynamic Monitoring', 'Pressure Sensing', 'Drug Therapy']),
            'enrollment_status': random.choice(['Enrolled', 'Completed', 'Withdrawn', 'Pending']),
            'age_group': random.choices(['0-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61+'], weights=[1, 1, 2, 5, 10, 20, 25])[0],
        })
    return pd.DataFrame(records)

def generate_dates(start_date=date(2019, 8, 1), end_date=date(2021, 11, 30)):
    records = []
    current =start_date
    us_holidays = holidays.USA()
    while current <= end_date:
        records.append({
            'date_id': int(current.strftime('%Y%m%d')),
            'full_date': current,
            'year': current.year,
            'month': current.month,
            'day': current.day,
            'quarter': (current.month - 1) // 3 + 1,
            'day_of_week': current.strftime('%A'),
            'is_holiday': current in us_holidays,
            'holiday_name': us_holidays.get(current, None)
        })
        current += timedelta(days=1)
    return pd.DataFrame(records)


manufacturers = generate_manufacturers()
devices = generate_devices(manufacturers)
trial_sites = generate_trial_sites()
patients = generate_patients(trial_sites)
dates = generate_dates()
print(dates[dates['is_holiday'] == True])