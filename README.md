# Reusable Big Data Analytics Framework on AWS

A configurable, event-driven Big Data analytics framework deployed on AWS. The same orchestration and processing core is reused across two sectors with very different data scales and analytical goals, and the sector-specific behaviour is defined entirely through declarative YAML configuration instead of code changes.

- **Energy** — daily electricity-consumption forecasting (conventional scale).
- **Telecommunications** — network-traffic anomaly detection over the Milan dataset (Big Data scale).

The project demonstrates that a single ingestion, transformation, orchestration and reporting core can serve both sectors, while each sector keeps its own configuration, access policies and analytical layer.

## Repository layout

```
framework-core/        Reusable core, identical in both deployments
  orchestration/       Step Functions definition (ASL)
  etl/                 Generic config-driven ETL engine
  lambdas/             Eight orchestration and support Lambda functions
  iam/                 Shared IAM: policy generator, trust and orchestration policies
  config/              Universal YAML template (sector configs derive from it)
  deploy/              Deployment scripts and the step-by-step guide (DEPLOY.md)

data/                  Public reference data and data sources (see data/README.md)

docs/                  Architecture and state-machine documentation

energy/                Energy-specific artifacts
  config/              Sector YAML (declarative configuration)
  iam/                 Energy data-access policies
  data-quality/        ETL data-quality report
  reports/             Generated executive reports
  dashboards/          Dashboard screenshots

telecom/               Telecom-specific artifacts
  config/              Sector YAML (declarative configuration)
  iam/                 Telecom data-access policies
  data-quality/        ETL data-quality report
  reports/             Generated executive reports
  dashboards/          Dashboard screenshots
```

## How reuse works

The core is the same file in both sectors: the ETL engine, the eight Lambda functions, the Step Functions definition and the shared IAM role. What changes between deployments is the sector YAML, which declares the data contract, the ETL recipe, the compute size, the analytical layer and the reporting prompt. The ETL engine reads those rules at run time, so adapting the framework to a new sector means editing configuration, not source code.

Each sector keeps its own:

- **YAML configuration** — data contract, ETL steps, compute, analytics layer, reporting.
- **Data-access IAM policies** — generated per sector from the YAML (least privilege).
- **Analytical layer** — Amazon RDS (PostgreSQL) for energy, Amazon Athena for telecom.

## Deployment overview

1. Upload the sector YAML and the ETL engine to the configuration bucket.
2. Create the Glue jobs (ETL) and the Lambda functions.
3. Generate the IAM policies for the sector with the policy generator and attach the per-sector role.
4. Deploy the Step Functions state machine from the shared ASL definition.

Infrastructure is provisioned manually in the AWS console for this project; automating it with Infrastructure as Code is listed as future work.

## Scope of this repository

Included: the deployment artifacts — ETL engine, Lambda functions, orchestration, IAM and configuration — the public exogenous data, the data-quality reports produced by the ETL, the generated executive reports and the dashboard screenshots.

Not included: the machine-learning model scripts and their outputs, the trained model artifact, and the raw datasets. The telecom raw data is public and referenced in `data/README.md`; the energy consumption data is private (non-disclosure agreement) and is not published.
