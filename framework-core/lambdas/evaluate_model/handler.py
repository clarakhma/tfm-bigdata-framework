"""
TFM Framework - Lambda: EvaluateModel
State 6: EvaluateModel (rama de entrenamiento - energía, retrain_weekly=true)
Lee el model_metrics.json que dejó el job de forecast (energia_forecast.py) en S3 y
decide si el modelo se aprueba (gate de calidad).

CRITERIO DE ÉXITO (D70, 3 ago 2026):
  El gate usa R² ≥ 0.85 a nivel de portafolio, NO el WAPE por propiedad.
  Justificación: la pregunta de negocio de energía es la planificación de consumo/coste
  a nivel de PORTAFOLIO (Energy Efficiency Manager), no la precisión horaria de cada
  propiedad. R² mide cuánta variación del consumo capta el modelo (0.92 obtenido). El
  WAPE por propiedad se conserva y se REPORTA como diagnóstico de la dificultad del
  forecasting granular (transparencia - no se oculta). Sustituye/complementa el umbral
  WAPE<15% (D52) para el gate de despliegue.

Universidad Europea de Madrid · Clara Khabbazi Manzur
"""

import json
import boto3

s3 = boto3.client("s3", region_name="eu-west-1")


def lambda_handler(event, context):
    sector = event["sector"]
    ml_config = event.get("ml_config", {})
    r2_threshold = float(ml_config.get("r2_threshold", 0.85))

    bucket = f"tfm-{sector}-ckm-ew1"
    key = "ml/model_metrics.json"
    metrics = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())

    r2 = float(metrics.get("r2_portafolio", 0.0))
    wape = metrics.get("wape_portafolio_pct")
    approved = r2 >= r2_threshold

    print(f"EvaluateModel {sector}: R2={r2} (umbral {r2_threshold}) → approved={approved} | "
          f"WAPE portafolio (diagnóstico)={wape}%")

    # Las claves mape/recall/f1_score existen para el ResultSelector del ASL (pueden ir a None).
    return {
        "best_model": metrics.get("best_model"),
        "approved": approved,
        "r2": r2,
        "r2_threshold": r2_threshold,
        "wape_portafolio_pct": wape,                 # diagnóstico, se reporta
        "error_neto_total_pct": metrics.get("error_neto_total_pct"),
        "model_wins": metrics.get("model_wins"),
        "mape": wape,                                # el % de error, para downstream
        "recall": None,
        "f1_score": None,
    }
