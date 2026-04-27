import os
import logging
import json
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import pubsub 
from apache_beam.io.kafka import ReadFromKafka
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'edwards-platform')
DATASET = 'edwards_dw'
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS', 'localhost:9092')
CONFLUENT_API_KEY = os.environ.get('CONFLUENT_API_KEY')
CONFLUENT_API_SECRET = os.environ.get('CONFLUENT_API_SECRET')
PUBSUB_SUBSCRIPTION = f'projects/{PROJECT_ID}/subscriptions/device-events-sub'

class DataQualityDoFn(beam.DoFn):
    GOOD = "good"
    DEAD_LETTER = 'dead_letter'

    REQUIRED_FIELDS = [ 'patient_id', 'device_id', 'trial_site_id',
      'reading_id', 'reading_type','heart_rate', 'systolic_pressure',
      'diastolic_pressure','pressure_gradient', 'battery_level', 'signal_strength',
      ]

    RANGE_RULES = {'heart_rate': (0, 300),
      'systolic_pressure': (0, 300),
      'pressure_gradient': (0, 300),
      }

    def process(self, record):
        try:
            #null value check
            for field in self.REQUIRED_FIELDS:
                if field not in record or (record[field] is None or str(record[field]).strip() == ''):
                    yield pvalue.TaggedOutput(self.DEAD_LETTER, {**record, '_fail_reason': f'null_{field}'})
                    return

            # Range validation
            for field, (min_val, max_val) in self.RANGE_RULES.items():
                if field in record:
                    if not (min_val <= record[field] <= max_val):
                        yield pvalue.TaggedOutput(self.DEAD_LETTER, {**record, '_fail_reason': f'range_{field}'})
                        return

            # Deduplication handled at pipeline level - emit good record
            yield pvalue.TaggedOutput(self.GOOD, record)
        except Exception as e:
            yield pvalue.TaggedOutput(self.DEAD_LETTER, {**record, '_fail_reason': f'exception: {str(e)}'})


class TransformTelemetryDoFn(beam.DoFn):
    def process(self, record):
        yield {
            'reading_id': record['reading_id'],
            'device_id': record['device_id'],
            'patient_id': record['patient_id'],
            'trial_site_id': record['trial_site_id'],
            'reading_type': record['reading_type'],
            'timestamp': record['event_ts'],
            'heart_rate': record['heart_rate'],
            'systolic_pressure': record['systolic_pressure'],
            'diastolic_pressure': record['diastolic_pressure'],
            'pressure_gradient': record['pressure_gradient'],
            'battery_level': record['battery_level'],
            'signal_strength': record['signal_strength']
        }

class TransformDeviceEventDoFn(beam.DoFn):
    def process(self, record):
        event_ts = datetime.fromisoformat(record['event_ts'])
        yield{
            'event_id': record['event_id'],
            'patient_id': record['patient_id'],
            'trial_site_id': record['trial_site_id'],
            'date_id': int(event_ts.strftime('%Y%m%d')),
            'timestamp': record['event_ts'],
            'event_type': record['event_type'],
            'outcome': None,
            'physician_notes': record.get('description'),
            'follow_up_required': None
        }

def passes_quality(record):
    for field in DataQualityDoFn.REQUIRED_FIELDS:
        if field not in record or record[field] is None or str(record[field]).strip() == '':
            return False
    for field, (min_val, max_val) in DataQualityDoFn.RANGE_RULES.items():
        if field in record:
            try:
                if not (min_val <= float(record[field]) <= max_val):
                    return False
            except (ValueError, TypeError):
                return False
    return True


def run_kafka_pipeline(pipeline):
    telemetry = (
        pipeline
        | 'ReadFromKafka' >> ReadFromKafka(
            consumer_config={
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_API_KEY}" password="{CONFLUENT_API_SECRET}";',
                'ssl.endpoint.identification.algorithm': 'https',
                'group.id': 'edwards-beam-consumer',
                'auto.offset.reset': 'earliest'
            },
            topics=['device-telemetry-raw']
        )
        | 'DecodeKafka' >> beam.Map(lambda record: json.loads(record[1].decode('utf-8')))
    )

    good = telemetry | 'FilterGood' >> beam.Filter(passes_quality)
    bad = telemetry | 'FilterBad' >> beam.Filter(lambda r: not passes_quality(r))

    (
        good
        | 'TransformTelemetry' >> beam.ParDo(TransformTelemetryDoFn())
        | 'WriteTelemetryToBQ' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.fct_device_readings',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )

    (
        bad
        | 'DeadLetterTelemetry' >> beam.Map(json.dumps)
        | 'WriteDeadLetterTelemetry' >> beam.io.WriteToText(
            f'gs://edwards-datalake/dead_letter/streaming/telemetry',
            file_name_suffix='.jsonl',
            triggering_frequency=30
        )
    )


def run_pubsub_pipeline(pipeline):
    events = (
        pipeline
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        | 'DecodePubSub' >> beam.Map(lambda record: json.loads(record.decode('utf-8')))
    )

    (
        events
        | 'TransformDeviceEvent' >> beam.ParDo(TransformDeviceEventDoFn())
        | 'WriteEventsToBQ' >> beam.io.WriteToBigQuery(
            f'{PROJECT_ID}:{DATASET}.fct_clinical_events',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )
    )

    (
        events
        | 'DeadLetterEvents' >> beam.Map(json.dumps)
        | 'WriteDeadLetterEvents' >> beam.io.WriteToText(
            f'gs://edwards-datalake/dead_letter/streaming/events',
            file_name_suffix='.jsonl',
            triggering_frequency=30
        )
    )

if __name__ == '__main__':
    options = PipelineOptions(
        project=PROJECT_ID,
        runner='DataflowRunner',
        region='us-central1',
        worker_zone='us-central1-b',
        max_num_workers=1,
        machine_type='e2-standard-2',
        temp_location='gs://edwards-datalake/temp',
        staging_location='gs://edwards-datalake/staging',
        job_name='edwards-streaming-pipeline',
        streaming=True,
        save_main_session=True,
        environment_variables={
            'CONFLUENT_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS,
            'CONFLUENT_API_KEY': CONFLUENT_API_KEY,
            'CONFLUENT_API_SECRET': CONFLUENT_API_SECRET
        }
    )

    with beam.Pipeline(options=options) as p:
        run_kafka_pipeline(p)
        run_pubsub_pipeline(p)
