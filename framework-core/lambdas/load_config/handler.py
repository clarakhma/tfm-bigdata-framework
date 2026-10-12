"""
TFM Framework — Lambda: LoadConfig
State 1: LoadConfig (first state in both pipelines)
Reads the sector YAML from S3 and returns the full config as a dict.
This is the ONLY state that differs between sectors — it loads different YAMLs.

Universidad Europea de Madrid · Clara Khabbazi Manzur
Key principle: reusability via config (12-Factor App, Principle III)
"""

import boto3
import yaml
import json

s3 = boto3.client('s3', region_name='eu-west-1')

def lambda_handler(event, context):
    """
    Input:
        event['sector']        — 'energia' or 'telecom'
        event['config_bucket'] — S3 bucket where YAML files live
        event['week']          — ISO week string e.g. '2026-W28'

    Output:
        Full config dict from the YAML — passed downstream to all states
    """
    sector = event['sector']
    config_bucket = event.get('config_bucket', 'tfm-config-bucket')
    week = event['week']

    config_key = f"config/sector_{sector}.yaml"

    # Read YAML from S3
    response = s3.get_object(Bucket=config_bucket, Key=config_key)
    config_raw = response['Body'].read().decode('utf-8')
    config = yaml.safe_load(config_raw)

    # Inject runtime parameters
    config['runtime'] = {
        'week': week,
        'sector': sector
    }

    # YAML convierte fechas SIN comillas (p.ej. `holidays: - 2023-12-06`) en objetos
    # datetime.date, que NO son JSON-serializables → Lambda falla al devolver la respuesta
    # (Runtime.MarshalError). Normalizamos: json.dumps con default=str convierte cualquier
    # tipo no serializable (como date) a texto, y json.loads lo vuelve a dict limpio.
    config = json.loads(json.dumps(config, default=str))

    print(f"Config loaded: sector={sector}, week={week}, glue_type={config['pipeline']['glue_job_type']}")
    return config
