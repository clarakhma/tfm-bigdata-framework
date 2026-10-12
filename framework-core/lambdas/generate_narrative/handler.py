"""
TFM Framework — Lambda: GenerateNarrative (GENÉRICO / sin lógica de sector)
==========================================================================
State 12: GenerateNarrative
Llama a Amazon Bedrock (Nova Pro) para generar el informe ejecutivo.

REUTILIZACIÓN HONESTA (D72, 3 ago 2026)
---------------------------------------
Este Lambda NO contiene ningún prompt ni ninguna rama `if sector==...`. El prompt
COMPLETO (rol, secciones, idioma) vive en el YAML del sector
(`executive_reporting.prompt_template`). El código solo:
  1) toma la plantilla de texto del YAML,
  2) sustituye los marcadores {kpi_summary}, {ml_results}, {anticipation}, ... por los
     datos reales del pipeline (anti-alucinación: el modelo solo ve estos números),
  3) invoca Bedrock.
Es idéntico entre sectores porque es DE VERDAD genérico — no porque empaquete los dos
prompts. (Corrige la "trampa" anterior de meter ambos sectores en el mismo archivo.)

Universidad Europea de Madrid · Clara Khabbazi Manzur
"""

import boto3
import json

bedrock = boto3.client('bedrock-runtime', region_name='eu-west-1')


def lambda_handler(event, context):
    """
    Input (desde Step Functions):
        event['prompt_template'] — plantilla de texto del YAML (executive_reporting.prompt_template)
        event['kpi_summary'], event['ml_results'], event['historico'], event['anticipation']
        event['reporting']       — bloque reporting (slm_model, stakeholder, language)
    Output:
        {'narrative': str, 'model_used': str}
    """
    reporting = event.get('reporting', {})
    model_id = (event.get('slm_model') or reporting.get('slm_model')
                or event.get('bedrock_model_id') or 'eu.amazon.nova-pro-v1:0')
    template = event.get('prompt_template')
    if not template:
        raise ValueError("Falta 'prompt_template' (declarado en el YAML: executive_reporting.prompt_template).")

    kpi_summary = event.get('kpi_summary', {}) or {}

    # Marcadores disponibles para la plantilla del YAML (genérico, sin lógica de sector).
    fields = {
        'sector': event.get('sector', ''),
        'stakeholder': event.get('stakeholder') or reporting.get('stakeholder', ''),
        'language': event.get('language') or reporting.get('language', 'es'),
        'week': kpi_summary.get('week', ''),
        'kpi_summary': json.dumps(kpi_summary, ensure_ascii=False, indent=2),
        'ml_results': json.dumps(event.get('ml_results', {}) or {}, ensure_ascii=False, indent=2),
        'historico': json.dumps(event.get('historico', []) or [], ensure_ascii=False, indent=2),
        'anticipation': json.dumps(event.get('anticipation', {}) or {}, ensure_ascii=False, indent=2),
    }

    # Sustitución segura por marcadores {clave} (no usa str.format → tolera llaves del JSON).
    prompt = template
    for k, v in fields.items():
        prompt = prompt.replace('{' + k + '}', str(v))

    response = bedrock.invoke_model(
        modelId=model_id,
        body=json.dumps({
            "schemaVersion": "messages-v1",
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"maxTokens": 1500, "temperature": 0.3, "topP": 0.9}
        }),
        contentType='application/json',
        accept='application/json'
    )
    result = json.loads(response['body'].read())
    narrative = result['output']['message']['content'][0]['text']
    return {'narrative': narrative, 'model_used': model_id}
