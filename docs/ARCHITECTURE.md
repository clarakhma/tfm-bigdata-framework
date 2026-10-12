# Architecture

The framework is a configurable, event-driven analytics pipeline on AWS. A single processing and orchestration core serves any sector; the sector-specific behaviour is declared in a YAML file instead of being written in code.

## Design principle: code vs configuration

The core is identical in every deployment: one ETL engine, eight Lambda functions, one Step Functions definition and the shared IAM role. What changes per sector is the YAML configuration, the data-access policies and the analytical layer. Onboarding a new sector means editing configuration, not source code.

The configuration follows an eleven-layer template (Layer 0 to Layer 10): identity, data contract, infrastructure and FinOps, quality gates, pipeline targets, declarative ETL, ML governance, KPIs and analytics, reporting, governance and lineage, and storage and security.

## Data layers (DIKW)

Each sector bucket is organised as prefixes that map to the Data–Information–Knowledge–Wisdom model:

- `raw/` — ingested source data.
- `processed/` — the descriptive, cleaned dataset (the dashboard flat table).
- `ml_input/` — the model-ready dataset.
- `ml/` — model outputs (predictions, metrics, artifacts).
- `quality/` — the ETL data-quality reports.

## Components

- **Amazon S3** — storage for every data layer.
- **AWS Glue** — the ETL engine. It runs as Python Shell for conventional volumes or PySpark for Big Data, selected from the YAML.
- **AWS Lambda** — eight generic functions for configuration, validation, metric aggregation, evaluation, narrative generation, PDF creation, report sending and dashboard update.
- **AWS Step Functions** — orchestrates the workflow; the same definition drives both sectors.
- **Amazon RDS (PostgreSQL)** or **Amazon Athena** — the analytical layer, chosen per sector.
- **Amazon QuickSight** — the descriptive dashboard.
- **Amazon Bedrock (Nova Pro)** — generates the executive report in natural language.
- **Amazon SES** — delivers the report by email.

## The two validation sectors

- **Energy** — daily electricity-consumption forecasting. Conventional scale (~200K rows), Glue Python Shell, RDS as the analytical layer. The model retrains weekly.
- **Telecommunications** — network-traffic anomaly detection over the Milan grid. Big Data scale (~320M rows), Glue PySpark, Athena as the analytical layer. The model is trained once and reused; the weekly job only scores.

## Analytical coverage

For each sector the pipeline covers the descriptive level (dashboard), the diagnostic level (drill-down queries), the predictive level (the ML model) and a decision-support level (the natural-language executive report).
