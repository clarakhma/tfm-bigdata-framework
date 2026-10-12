# Deployment guide

This guide explains how to deploy the framework for a sector, and how to onboard a **new** sector reusing the same core. The infrastructure is provisioned manually in the AWS console; these steps and scripts document and partially automate that process (full Infrastructure as Code is listed as future work).

The core artifacts (ETL engine, the eight Lambda functions, the Step Functions definition and the shared IAM) are the same for every sector. Only the sector YAML, the data-access policies and the analytical layer change.

## Scripts in this folder

| Script | Purpose |
|---|---|
| `deploy_pipeline.ps1` | Packages and uploads the eight Lambda functions. |
| `build_weasyprint_layer.sh` | Builds the Lambda layer used by the PDF report function. |

## Onboard a new sector

### 1. Configuration
1. Copy `config/_TEMPLATE_universal.yaml` to `config/sector_<name>_declarative.yaml`.
2. Fill in the values: data contract, ETL steps, compute size, analytics engine (RDS or Athena), reporting prompt and security. No code is changed.
3. Upload the sector YAML and the shared `etl/etl_engine.py` to the configuration bucket (`s3://tfm-config-ckm-ew1/config/` and `/scripts/`).

### 2. Storage (S3)
1. Create the sector bucket `s3://tfm-<name>-ckm-ew1` with the DIKW prefixes (`raw/`, `processed/`, `ml_input/`, `ml/`, `quality/`, `tmp/`).
2. Add the S3 Gateway Endpoint so the pipeline reaches S3 privately, without a NAT gateway.

### 3. IAM
1. Generate the sector policy: `python iam/build_policy.py <name>` (reads the sector YAML).
2. Create the per-sector execution role `tfm-<name>-pipeline-role` with the generated policy and the trust relationships in `iam/trust-*.json`.
3. The Step Functions orchestration role (`iam/policy-stepfunctions.json`, `iam/policy-sfn-glue.json`) is shared and only needs to exist once.

### 4. ETL job (Glue)
1. Create a Glue job pointing its script location to the shared `etl_engine.py`.
2. Choose the engine from the YAML: `python_shell` (small scale) or `pyspark` (Big Data).
3. Add the required Python modules (for example `pyyaml,pyarrow,s3fs`).

### 5. Lambda functions
1. Run `deploy_pipeline.ps1` to package and upload the eight functions.
2. Assign each function the **sector** execution role (never another sector's role).
3. For the PDF function, build and attach the layer with `build_weasyprint_layer.sh`.

### 6. Analytical layer
- **RDS** (relational): create the PostgreSQL instance and the flat table; the processed data is loaded by the `aggregate_metrics` Lambda.
- **Athena** (serverless): create the external tables over the `processed/` and anomalies prefixes with partition projection.

### 7. Machine learning (sector-specific)
1. Train the model once (bootstrap) and upload the artifact to the sector `ml/model/` prefix.
2. Set `retrain_weekly` in the YAML: `true` retrains every week, `false` loads the pre-trained model and only scores.

### 8. Orchestration (Step Functions)
1. Create the state machine from `orchestration/state_machine.asl.json`.
2. Deploy it once per sector; the definition is identical, the behaviour is driven by the sector YAML.

### 9. Reporting
1. Enable the Bedrock model used in the YAML.
2. Verify the SES sender identity for the report email.

### 10. Run
Start the state machine with the sector and the target week as input. The pipeline validates, runs the ETL, trains or scores, aggregates the KPIs, generates the narrative and the PDF, and sends the report.
