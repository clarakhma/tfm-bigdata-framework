"""
TFM Framework — Lambda: UpdateDashboard (State 9)
Escribe los KPIs agregados a S3 (los lee QuickSight) y, SOLO si el YAML declara un dataset SPICE,
lanza el refresco (create_ingestion). En Direct Query (telecom) no hay dataset id → se salta el refresco;
si lo hubiera pero el dataset es Direct Query, el error se captura y el pipeline continúa.

Byte-idéntico entre sectores: energía (SPICE) refresca; telecom (Direct Query) se salta.
Universidad Europea de Madrid · Clara Khabbazi Manzur
"""
import boto3
import json
from datetime import datetime

s3 = boto3.client('s3', region_name='eu-west-1')


def lambda_handler(event, context):
    sector = event['sector']
    kpi_summary = event['kpi_summary']
    alerts = event.get('alerts', [])
    dashboard = event['dashboard']
    week = event['runtime']['week']

    # output_bucket 's3://bucket/dashboard/' → separar bucket y prefijo
    raw = dashboard['output_bucket'].replace('s3://', '').rstrip('/')
    bucket = raw.split('/')[0]
    prefix = '/'.join(raw.split('/')[1:]) or 'dashboard'

    # 1. Escribir KPIs a S3 (QuickSight los lee)
    kpi_key = f"{prefix}/{week}/kpis.json"
    alerts_key = f"{prefix}/{week}/alerts.json"
    s3.put_object(Bucket=bucket, Key=kpi_key,
                  Body=json.dumps(kpi_summary, ensure_ascii=False, default=str),
                  ContentType='application/json')
    s3.put_object(Bucket=bucket, Key=alerts_key,
                  Body=json.dumps(alerts, ensure_ascii=False, default=str),
                  ContentType='application/json')

    # 2. Refresco SPICE SOLO si el YAML declara un dataset (energía). Direct Query (telecom) → sin id → se salta.
    ingestion_id = None
    dataset_id = dashboard.get('quicksight_dataset_id')
    if dataset_id:
        try:
            ingestion_id = f"tfm-{sector}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            account_id = context.invoked_function_arn.split(':')[4]
            boto3.client('quicksight').create_ingestion(
                DataSetId=dataset_id, IngestionId=ingestion_id, AwsAccountId=account_id)
            print(f"SPICE refresh lanzado: {ingestion_id}")
        except Exception as e:
            # Direct Query u otro → NO rompe el pipeline (el dashboard lee Athena en vivo).
            print(f"SPICE refresh omitido ({type(e).__name__}): {e}")
            ingestion_id = None

    print(f"Dashboard updated: sector={sector}, week={week}, kpis=s3://{bucket}/{kpi_key}")
    return {
        'status': 'ok',
        'dashboard_updated': True,
        's3_kpi_path': f"s3://{bucket}/{kpi_key}",
        's3_alerts_path': f"s3://{bucket}/{alerts_key}",
        'quicksight_ingestion_id': ingestion_id,
    }
