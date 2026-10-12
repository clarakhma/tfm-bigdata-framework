"""
TFM Framework - Lambda: ValidateData
State 2: ValidateData
Validates that the source data file(s) exist in S3 and pass basic quality checks.
Runs BEFORE Glue ETL to fail fast and avoid wasted compute.

Universidad Europea de Madrid · Clara Khabbazi Manzur
"""

import boto3
import json
from datetime import datetime

s3 = boto3.client('s3', region_name='eu-west-1')

def lambda_handler(event, context):
    """
    Input:
        event['sector']          - 'energia' or 'telecom'
        event['pipeline']        - pipeline config dict (from LoadConfig)
        event['runtime']['week'] - ISO week string e.g. '2026-W28'

    Output:
        {
            'valid': True/False,
            'file_count': int,
            'total_size_mb': float,
            'errors': [str]      - empty list if valid
        }

    Raises:
        Exception if validation fails (Step Functions marks state FAILED)
    """
    sector = event['sector']
    pipeline = event['pipeline']
    week = event['runtime']['week']
    # source_bucket may be 's3://bucket/raw/' - split bucket from any leading prefix
    raw = pipeline['source_bucket'].replace('s3://', '').rstrip('/')
    source_bucket = raw.split('/')[0]
    base_prefix = '/'.join(raw.split('/')[1:])          # e.g. 'raw'

    errors = []
    total_size = 0
    file_count = 0

    # Check sector-specific location. Energia loads one weekly CSV file (source_file in
    # the YAML); telecom drops a large TSV under a week prefix.
    # Config-driven (SIN rama de sector): si el YAML declara `source_file` → valida ese archivo
    # único (energía); si no, valida que el prefijo raw/ tenga datos (telecom particiona por
    # periodo, no por semana). El límite de tamaño también sale del YAML.
    source_file = pipeline.get('source_file')
    if source_file:
        prefix = f"{base_prefix}/{source_file}"
    else:
        prefix = f"{base_prefix}/" if base_prefix else ""
    min_files = 1
    max_size_mb = pipeline.get('max_size_mb')   # None → sin tope superior (opcional, declarado en YAML)

    # List objects in S3 source prefix
    try:
        response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
        objects = response.get('Contents', [])
        file_count = len(objects)
        total_size = sum(obj['Size'] for obj in objects) / (1024 * 1024)  # bytes → MB
    except Exception as e:
        errors.append(f"Cannot list S3 bucket {source_bucket}/{prefix}: {str(e)}")

    # Validations
    if file_count < min_files:
        errors.append(f"Expected at least {min_files} file(s), found {file_count}")

    if max_size_mb and file_count > 0 and total_size > max_size_mb:
        errors.append(f"Total file size {total_size:.1f} MB exceeds maximum {max_size_mb} MB")

    if errors:
        raise ValueError(f"Data validation failed for sector={sector}, week={week}: {errors}")

    result = {
        'valid': True,
        'file_count': file_count,
        'total_size_mb': round(total_size, 2),
        'source_prefix': f"s3://{source_bucket}/{prefix}",
        'errors': []
    }

    print(f"Validation passed: sector={sector}, week={week}, files={file_count}, size={total_size:.1f} MB")
    return result
