<#
=============================================================================
 Despliegue del pipeline - TFM Big Data Framework (REUSABLE por sector)
=============================================================================
 MISMO script para los DOS sectores (SO4: reuso por configuración). El sector
 entra por parámetro y decide el rol, los nombres de función, el YAML y la
 State Machine. El código de las Lambdas es idéntico - solo cambia la config.

 Empaqueta y despliega las 8 Lambdas del sector, sube su YAML a S3 y
 crea/actualiza su State Machine. Los estados pesados (ETL, Train, BatchTransform)
 son Pass en el ASL, así que el pipeline corre entero SIN Glue ni SageMaker.

 Requisitos:
   - AWS CLI configurado (aws configure).
   - Python + pip en el PATH (para empaquetar dependencias Linux).
   - El rol tfm-<sector>-pipeline-role y el rol tfm-stepfunctions-role ya creados
     (ver docs/DEPLOY_PIPELINE.md, paso 2).

 Uso (desde la raíz del repo):
   pwsh deploy/deploy_pipeline.ps1                 # energía (por defecto)
   pwsh deploy/deploy_pipeline.ps1 -Sector telecom # telecom
=============================================================================
#>
param([ValidateSet("energia","telecom")][string]$Sector = "energia")

$ErrorActionPreference = "Stop"
$REGION   = "eu-west-1"
$ACCOUNT  = "<ACCOUNT_ID>"
$ROLE     = "arn:aws:iam::${ACCOUNT}:role/tfm-${Sector}-pipeline-role"
$SF_ROLE  = "arn:aws:iam::${ACCOUNT}:role/tfm-stepfunctions-role"
$CFG_BUCKET = "tfm-config-ckm-ew1"
$SENDER   = "your-email@example.com"     # remitente verificado en SES
Write-Host "==> Sector objetivo: $Sector" -ForegroundColor Magenta

$REPO = (Resolve-Path "$PSScriptRoot/..").Path
$BUILD = Join-Path $REPO "deploy/build"
New-Item -ItemType Directory -Force -Path $BUILD | Out-Null

# --- función auxiliar: empaqueta un handler (+ deps Linux) en un zip ----------
function Package-Lambda {
    param([string]$Name, [string]$HandlerFile, [string[]]$Deps)
    $stage = Join-Path $BUILD $Name
    if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
    New-Item -ItemType Directory -Force -Path $stage | Out-Null

    Copy-Item $HandlerFile (Join-Path $stage (Split-Path $HandlerFile -Leaf))

    if ($Deps.Count -gt 0) {
        # Descarga wheels de LINUX (manylinux) aunque estemos en Windows
        pip install --platform manylinux2014_x86_64 --implementation cp `
            --python-version 3.12 --only-binary=:all: --target $stage $Deps | Out-Null
    }
    $zip = Join-Path $BUILD "$Name.zip"
    if (Test-Path $zip) { Remove-Item $zip }
    Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $zip
    return $zip
}

# --- función auxiliar: crea la Lambda si no existe, si existe actualiza -------
function Deploy-Lambda {
    param([string]$FnName, [string]$Zip, [string]$Handler, [int]$Timeout, [int]$Memory, [hashtable]$Env)

    $exists = $true
    try { aws lambda get-function --function-name $FnName --region $REGION *>$null }
    catch { $exists = $false }

    if ($exists) {
        Write-Host "  actualizando $FnName ..."
        aws lambda update-function-code --function-name $FnName `
            --zip-file "fileb://$Zip" --region $REGION | Out-Null
        aws lambda wait function-updated --function-name $FnName --region $REGION
        aws lambda update-function-configuration --function-name $FnName `
            --handler $Handler --timeout $Timeout --memory-size $Memory `
            --runtime python3.12 --region $REGION | Out-Null
    } else {
        Write-Host "  creando $FnName ..."
        aws lambda create-function --function-name $FnName `
            --runtime python3.12 --role $ROLE --handler $Handler `
            --timeout $Timeout --memory-size $Memory `
            --zip-file "fileb://$Zip" --region $REGION | Out-Null
    }
    aws lambda wait function-updated --function-name $FnName --region $REGION
    if ($Env) {
        $pairs = ($Env.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ","
        aws lambda update-function-configuration --function-name $FnName `
            --environment "Variables={$pairs}" --region $REGION | Out-Null
    }
}

Write-Host "=== 1/4  Empaquetando y desplegando las 8 Lambdas ===" -ForegroundColor Cyan

# name suffix ; handler file ; handler string ; timeout ; memory ; deps ; env
$L = "$REPO/lambda"
$funcs = @(
  @{n="load-config";        f="$L/load_config/handler.py";        h="handler.lambda_handler";              t=30;  m=256; d=@("PyYAML")},
  @{n="validate-data";      f="$L/validate_data/handler.py";      h="handler.lambda_handler";              t=30;  m=256; d=@()},
  @{n="evaluate-model";     f="$L/evaluate_model/handler.py";     h="handler.lambda_handler";              t=30;  m=256; d=@()},
  @{n="aggregate-metrics";  f="$L/aggregate_metrics/handler.py";  h="handler.lambda_handler";              t=60;  m=512; d=@()},
  @{n="update-dashboard";   f="$L/update_dashboard/handler.py";   h="handler.lambda_handler";              t=30;  m=256; d=@()},
  @{n="generate-narrative"; f="$L/generate_narrative/handler.py"; h="handler.lambda_handler";              t=120; m=512; d=@()},
  @{n="generate-pdf";       f="$L/generate_pdf/handler.py";       h="handler.lambda_handler";              t=120; m=1024;d=@("reportlab")},
  @{n="send-report";        f="$L/send_report/handler.py";        h="handler.lambda_handler";              t=60;  m=256; d=@()}
)

foreach ($fn in $funcs) {
    $zip = Package-Lambda -Name $fn.n -HandlerFile $fn.f -Deps $fn.d
    $env = $null
    if ($fn.n -eq "send-report") { $env = @{ SENDER_EMAIL = $SENDER } }
    Deploy-Lambda -FnName "tfm-${Sector}-$($fn.n)" -Zip $zip -Handler $fn.h -Timeout $fn.t -Memory $fn.m -Env $env
}

Write-Host "=== 2/4  Subiendo el YAML de configuración a S3 ===" -ForegroundColor Cyan
aws s3 cp "$REPO/config/sector_${Sector}.yaml" "s3://$CFG_BUCKET/config/sector_${Sector}.yaml" --region $REGION | Out-Null
Write-Host "  s3://$CFG_BUCKET/config/sector_${Sector}.yaml"

Write-Host "=== 3/4  Creando/actualizando la State Machine ===" -ForegroundColor Cyan
$asl = "$REPO/step_functions/state_machine.asl.json"
$smName = "tfm-pipeline-${Sector}"
$smArn = "arn:aws:states:${REGION}:${ACCOUNT}:stateMachine:$smName"
$smExists = $true
try { aws stepfunctions describe-state-machine --state-machine-arn $smArn --region $REGION *>$null }
catch { $smExists = $false }

if ($smExists) {
    aws stepfunctions update-state-machine --state-machine-arn $smArn `
        --definition "file://$asl" --role-arn $SF_ROLE --region $REGION | Out-Null
    Write-Host "  actualizada: $smArn"
} else {
    aws stepfunctions create-state-machine --name $smName `
        --definition "file://$asl" --role-arn $SF_ROLE --type STANDARD --region $REGION | Out-Null
    Write-Host "  creada: $smArn"
}

Write-Host "=== 4/4  Listo ===" -ForegroundColor Green
Write-Host "Lanza una ejecución con:" -ForegroundColor Yellow
Write-Host "  aws stepfunctions start-execution ``"
Write-Host "    --state-machine-arn $smArn ``"
Write-Host "    --input '{\""sector\"":\""$Sector\"",\""week\"":\""2024-W40\""}' ``"
Write-Host "    --region $REGION"
