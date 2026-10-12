# State machine (AWS Step Functions)

The pipeline is orchestrated by a single Amazon States Language (ASL) definition, deployed once per sector. The same definition drives both sectors; its behaviour at run time depends on the parameters declared in the sector YAML. The definition is in `framework-core/orchestration/state_machine.asl.json`.

## Workflow states

| State | Type | What it does |
|---|---|---|
| **LoadConfig** | Lambda | Reads the sector YAML from S3 and injects the target week window into the run. |
| **ValidateData** | Lambda | Checks the raw data against the data contract (schema, required columns, size). |
| **RunETL** | Glue | Runs the shared `etl_engine.py` (EDA and ML recipes). Python Shell or PySpark, per the YAML. |
| **CheckShouldTrain** | Choice | Branches on `ml.retrain_weekly`: train, or go straight to inference. |
| **TrainModel** | Glue | Trains the sector model (only when the branch requires it). |
| **EvaluateModel** | Lambda | Computes the gate metric (for example WAPE) from the model output. |
| **EvalGate** | Choice | Approves or rejects the model against the YAML threshold. |
| **InferenceRouter** | Choice | Routes to the configured inference engine. |
| **GlueScore** | Glue | Applies the pre-trained model to score new data. |
| **SageMakerTransform** | Pass | Placeholder for a SageMaker inference path. |
| **AggregateMetrics** | Lambda | Runs the KPI queries against the analytical engine (RDS or Athena). |
| **UpdateDashboard** | Lambda | Writes the KPI and alert artifacts; refreshes SPICE only if a dataset id is configured. |
| **GenerateNarrative** | Lambda | Produces the executive narrative with Amazon Bedrock (Nova Pro). |
| **GeneratePDF** | Lambda | Builds the PDF report. |
| **SendReport** | Lambda | Sends the report by email with Amazon SES. |

## Terminal states

| State | Meaning |
|---|---|
| **PipelineSuccess** | The run finished successfully. |
| **ModelRejected** | The model did not pass the evaluation gate. |
| **PipelineFailed** | A state failed; reached through the error-catch paths. |

## Sector specialization through configuration

The orchestration logic is not rewritten for a second sector. The differences are driven by the YAML. For telecom, for example, `RunETL` runs a PySpark job instead of a Python Shell job, `CheckShouldTrain` reads `retrain_weekly: false` and skips the training branch, `InferenceRouter` routes to `GlueScore` where the Isolation Forest model is applied, and `AggregateMetrics` queries Athena instead of RDS. The definition stays the same; only the configuration changes.
