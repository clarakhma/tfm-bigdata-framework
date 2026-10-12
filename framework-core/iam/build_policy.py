#!/usr/bin/env python3
"""
TFM Framework - Generador ÚNICO de políticas IAM de datos (Tier B)
==================================================================
Universidad Europea de Madrid · Clara Khabbazi Manzur

QUÉ ES
------
Un solo archivo, byte-idéntico, que lee `config/sector_{X}.yaml` y EMITE la
política IAM del rol de datos de ese sector (el rol que las Lambdas del pipeline
usan para tocar el bucket del sector, Bedrock, SES y su capa analítica).

POR QUÉ ASÍ (argumento de reutilización - SO4)
----------------------------------------------
IAM es estático: no puede "leer el YAML en runtime" como las Lambdas. Por eso la
reutilización en IAM se hace en DESPLIEGUE: un mismo generador (este archivo,
mismo MD5) parametrizado por el YAML produce la política de cada sector. Lo único
que cambia es el OUTPUT, igual que los dos ETL salen del mismo patrón pero
producen datasets distintos.

    md5sum iam/build_policy.py   ->  UN solo hash
    python iam/build_policy.py telecom   ->  policy de telecom (Athena)
    python iam/build_policy.py energia   ->  policy de energia (RDS + KMS)

QUÉ VARÍA SEGÚN EL YAML
-----------------------
  bucket                      -> Resource de S3 del sector
  storage.analytical=athena   -> bloques Athena + Glue Data Catalog
  storage.analytical=rds      -> bloques Secrets Manager + VPC (ENI)
  security.encryption=SSE-KMS -> bloque KMS (la clave se resuelve desde el alias)
  reporting.slm_model         -> Resource de Bedrock (inference profile + FM)
  dashboard.quicksight_dataset_id -> bloque QuickSight (create_ingestion / SPICE refresh)

Lo que NO cambia (idéntico para todos) va en build_common_statements().
"""

import json
import sys
from pathlib import Path

import yaml

# Bucket de configuración compartido por TODOS los sectores (no es sector-específico).
CONFIG_BUCKET = "tfm-config-ckm-ew1"

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "iam" / "generated"


# ── Helpers de parsing del YAML ────────────────────────────────────────────────
def load_config(sector: str) -> dict:
    # El archivo canónico es el declarativo (derivado de _TEMPLATE_universal.yaml).
    path = REPO / "config" / f"sector_{sector}_declarative.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def account_from_role_arn(role_arn: str) -> str:
    # arn:aws:iam::<ACCOUNT_ID>:role/...  -> <ACCOUNT_ID>
    return role_arn.split(":")[4]


def bucket_name(cfg: dict) -> str:
    # "s3://tfm-telecom-ckm-ew1" -> "tfm-telecom-ckm-ew1"
    return cfg["bucket"].replace("s3://", "").rstrip("/")


def bedrock_resources(region: str, account: str, model_id: str) -> list:
    # model_id p.ej. "eu.amazon.nova-pro-v1:0"
    # inference profile = tal cual; foundation model = sin el prefijo regional ("eu.")
    fm = model_id.split(".", 1)[1] if model_id.split(".", 1)[0] in ("eu", "us", "apac") else model_id
    return [
        f"arn:aws:bedrock:{region}:{account}:inference-profile/{model_id}",
        f"arn:aws:bedrock:*::foundation-model/{fm}",
    ]


def resolve_kms_key_arn(alias: str, region: str, account: str) -> str:
    """Resuelve alias/tfm-energia-key -> ARN de la clave. IAM necesita el ARN de la
    key (o *), no el alias. Se resuelve en DESPLIEGUE con boto3 (deploy-time = cuando
    IAM lee la config). Si no hay credenciales, cae al ARN del alias con un aviso."""
    try:
        import boto3
        kms = boto3.client("kms", region_name=region)
        return kms.describe_key(KeyId=alias)["KeyMetadata"]["Arn"]
    except Exception as e:  # sin credenciales o alias inexistente
        print(f"  [aviso] no se pudo resolver {alias} via boto3 ({e}); "
              f"uso el ARN del alias. Regenera con credenciales para la key ARN real.")
        return f"arn:aws:kms:{region}:{account}:{alias}"


# ── Bloques comunes (idénticos para todos los sectores) ─────────────────────────
def build_common_statements(cfg: dict, region: str, account: str, bucket: str) -> list:
    model_id = cfg["reporting"]["slm_model"]
    return [
        {
            "Sid": "S3ConfigReadOnly",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{CONFIG_BUCKET}",
                f"arn:aws:s3:::{CONFIG_BUCKET}/*",
            ],
        },
        {
            "Sid": "S3SectorReadWrite",
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
            "Resource": [
                f"arn:aws:s3:::{bucket}",
                f"arn:aws:s3:::{bucket}/*",
            ],
        },
        {
            "Sid": "BedrockEUInferenceProfile",
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel"],
            "Resource": bedrock_resources(region, account, model_id),
        },
        {
            "Sid": "SESEmail",
            "Effect": "Allow",
            "Action": ["ses:SendEmail", "ses:SendRawEmail"],
            "Resource": f"arn:aws:ses:{region}:{account}:identity/*",
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            "Resource": f"arn:aws:logs:{region}:{account}:log-group:/aws/lambda/tfm-*",
        },
    ]


# ── Bloques según capa analítica (Athena vs RDS) ────────────────────────────────
def build_athena_statements(cfg: dict, region: str, account: str) -> list:
    db = cfg["storage"]["database"]
    bucket = bucket_name(cfg)
    return [
        {
            "Sid": "AthenaQueryExecution",
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution", "athena:GetQueryExecution",
                "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup",
            ],
            "Resource": f"arn:aws:athena:{region}:{account}:workgroup/primary",
        },
        {
            "Sid": "GlueCatalogForAthena",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                "glue:GetPartition", "glue:GetPartitions",
            ],
            "Resource": [
                f"arn:aws:glue:{region}:{account}:catalog",
                f"arn:aws:glue:{region}:{account}:database/{db}",
                f"arn:aws:glue:{region}:{account}:table/{db}/*",
            ],
        },
        {
            # Athena verifica el bucket de salida (GetBucketLocation) y escribe ahí los resultados de la query.
            "Sid": "AthenaResultsS3",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation", "s3:ListBucket", "s3:GetObject", "s3:PutObject",
                "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket}",
                f"arn:aws:s3:::{bucket}/*",
            ],
        },
    ]


def build_rds_statements(cfg: dict, region: str, account: str) -> list:
    secret = cfg["storage"]["secret_name"]
    return [
        {
            "Sid": "SecretsManagerRDS",
            "Effect": "Allow",
            "Action": ["secretsmanager:GetSecretValue"],
            "Resource": f"arn:aws:secretsmanager:{region}:{account}:secret:{secret}*",
        },
        {
            "Sid": "VPCNetworkInterfaceForRDS",
            "Effect": "Allow",
            "Action": [
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
            ],
            "Resource": "*",
        },
    ]


def build_quicksight_statement(cfg: dict, region: str, account: str) -> dict:
    """Permiso para que UpdateDashboard refresque el dataset SPICE (create_ingestion).
    Solo se emite si el YAML declara dashboard.quicksight_dataset_id (energía SPICE);
    telecom en Direct Query no lo necesita → no aparece en su política."""
    ds = cfg["dashboard"]["quicksight_dataset_id"]
    base = f"arn:aws:quicksight:{region}:{account}:dataset/{ds}"
    return {
        "Sid": "QuickSightSpiceRefresh",
        "Effect": "Allow",
        "Action": ["quicksight:CreateIngestion", "quicksight:DescribeIngestion", "quicksight:DescribeDataSet"],
        # CreateIngestion actúa sobre el sub-recurso ingestion; DescribeDataSet sobre el dataset.
        "Resource": [base, f"{base}/ingestion/*"],
    }


def build_kms_statement(cfg: dict, region: str, account: str) -> dict:
    alias = cfg["security"].get("kms_key_alias")
    key_arn = resolve_kms_key_arn(alias, region, account)
    return {
        "Sid": "KMSForEncryptedBucket",
        "Effect": "Allow",
        "Action": ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"],
        "Resource": key_arn,
    }


# ── Ensamblado ──────────────────────────────────────────────────────────────────
def build_policy(sector: str) -> dict:
    cfg = load_config(sector)
    region = cfg["region"]
    account = account_from_role_arn(cfg["security"]["iam_role"])
    bucket = bucket_name(cfg)

    statements = build_common_statements(cfg, region, account, bucket)

    analytical = cfg["storage"]["analytical"]
    if analytical == "athena":
        statements += build_athena_statements(cfg, region, account)
    elif analytical == "rds":
        statements += build_rds_statements(cfg, region, account)
    else:
        raise ValueError(f"storage.analytical desconocido: {analytical}")

    if cfg["security"].get("encryption") == "SSE-KMS":
        statements.append(build_kms_statement(cfg, region, account))

    # QuickSight SPICE refresh: solo si el YAML declara el dataset (energía). Config-driven.
    if cfg.get("dashboard", {}).get("quicksight_dataset_id"):
        statements.append(build_quicksight_statement(cfg, region, account))

    return {"Version": "2012-10-17", "Statement": statements}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("energia", "telecom"):
        print("Uso: python iam/build_policy.py <energia|telecom>")
        sys.exit(1)
    sector = sys.argv[1]
    policy = build_policy(sector)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"policy-{sector}-pipeline.generated.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(policy, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"OK -> {out_path}  ({len(policy['Statement'])} statements, analytical={load_config(sector)['storage']['analytical']})")


if __name__ == "__main__":
    main()
