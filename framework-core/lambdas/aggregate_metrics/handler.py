"""
TFM Framework — Lambda: AggregateMetrics (GENÉRICO / sin lógica de sector)
=========================================================================
State 8: calcula los KPIs del informe. Despacha por MOTOR (rds | athena | csv)
según `analytics_source.engine` del YAML. Byte-idéntico entre sectores:
  energía  → engine=rds    (SQL Postgres, inyecta {week_start/week_end/prev_week_start})
  telecom  → engine=athena (SQL Athena, inyecta {periodo}/{scope})

OPCIÓN B (17 ago): en energía este Lambda TAMBIÉN materializa la sábana processed/ en RDS
(reemplazo atómico) antes de consultar — la carga se movió aquí desde el ETL para evitar
el problema de pip/NAT en la VPC (los Lambdas usan layers pre-construidas). Telecom (athena)
se lo salta.

Despliegue (solo energía): layers psycopg2 + pyarrow + pandas · Lambda en la VPC · Secrets Manager.
Universidad Europea de Madrid · Clara Khabbazi Manzur
"""

import boto3
import csv
import io
import json
import time
from collections import defaultdict

s3 = boto3.client('s3')   # región desde AWS_REGION


def _fill(sql, vars):
    """Sustituye {clave} por su valor en el SQL (fechas de semana, scope, periodo)."""
    for k, v in vars.items():
        sql = sql.replace('{' + k + '}', str(v))
    return sql


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _rds_creds(storage):
    """Credenciales RDS. En VPC sin endpoint de Secrets Manager, se leen de variables de
    entorno (cifradas por Lambda); si no, de Secrets Manager. Mantiene la reutilización."""
    import os
    if os.environ.get('RDS_HOST'):
        return dict(host=os.environ['RDS_HOST'],
                    port=int(os.environ.get('RDS_PORT', storage.get('port', 5432))),
                    dbname=os.environ.get('RDS_DB') or storage.get('database'),
                    user=os.environ['RDS_USER'], password=os.environ['RDS_PASSWORD'])
    sec = json.loads(boto3.client('secretsmanager')
                     .get_secret_value(SecretId=storage['secret_name'])['SecretString'])
    return dict(host=sec['host'], port=int(sec.get('port', storage.get('port', 5432))),
                dbname=storage.get('database') or sec.get('dbname'),
                user=sec['username'], password=sec['password'])


def lambda_handler(event, context):
    sector = event.get('sector', '')
    runtime = event.get('runtime') or {}
    a = event['analytics_source']
    storage = event.get('storage', {})
    bucket = event.get('bucket', f"tfm-{sector}-ckm-ew1").replace('s3://', '').rstrip('/')

    vars = {
        'week': runtime.get('week', ''),
        'week_start': runtime.get('week_start', ''),
        'week_end': runtime.get('week_end', ''),
        'prev_week_start': runtime.get('prev_week_start', ''),
        'periodo': runtime.get('periodo') or event.get('periodo', ''),
        'scope': a.get('scope', '1=1'),
    }

    engine = a.get('engine', 'athena')
    if engine == 'rds':
        # OPCIÓN B: primero materializa la sábana processed/ → RDS (atómico), luego consulta.
        load_sabana_to_rds(storage, bucket)
        kpi_summary, alerts = run_rds(a, vars, storage)
    elif engine == 'athena':
        kpi_summary, alerts = run_athena(a, vars)
    elif engine == 'csv':
        kpi_summary, alerts = run_csv(a, bucket)
    else:
        raise ValueError(f"analytics_source.engine no soportado: {engine}")

    kpi_summary['week'] = vars['week']

    ml_results = {}
    mk = a.get('model_metrics_key')
    if mk:
        try:
            ml_results = json.loads(s3.get_object(Bucket=bucket, Key=mk)['Body'].read())
        except Exception as e:
            print(f"WARNING: no se pudo leer {mk}: {e}")

    forecast = load_forecast(a, bucket)

    print(f"AggregateMetrics [{engine}] sector={sector}: kpis={list(kpi_summary.keys())}, "
          f"forecast={'sí' if forecast else 'no'}, alerts={len(alerts)}")
    return {'kpi_summary': kpi_summary, 'ml_results': ml_results, 'forecast': forecast,
            'historico': [], 'alerts': alerts, 'anticipation': a.get('anticipation', {})}


# ── Carga de la sábana processed/ → RDS (reemplazo atómico) — solo engine=rds ────
def load_sabana_to_rds(storage, bucket):
    """Lee la sábana processed/ (parquet) y la carga en la tabla del dashboard con
    TRUNCATE + INSERT en UNA transacción (sin hueco). Layers: pandas + pyarrow + psycopg2."""
    src = storage.get('dashboard_source')
    table = storage.get('dashboard_table')
    if not src or not table:
        print("sin dashboard_source/dashboard_table → no se carga sábana")
        return 0
    import pandas as pd
    import psycopg2
    from psycopg2.extras import execute_values

    obj = s3.get_object(Bucket=bucket, Key=src)
    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

    conn = psycopg2.connect(**_rds_creds(storage))
    try:
        cols = list(df.columns)
        collist = ",".join(f'"{c}"' for c in cols)
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with conn:                                   # transacción: COMMIT al salir, ROLLBACK si falla
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE {table}')
                execute_values(cur, f'INSERT INTO {table} ({collist}) VALUES %s', rows, page_size=1000)
    finally:
        conn.close()
    print(f"sábana cargada a RDS {table}: {len(df)} filas")
    return len(df)


# ── Runner RDS (energía): SQL Postgres con inyección de fechas de semana ────────
def run_rds(a, vars, storage):
    import psycopg2
    conn = psycopg2.connect(**_rds_creds(storage))

    def q(sql):
        cur = conn.cursor()
        cur.execute(_fill(sql, vars))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    kpi = {}
    for name, sql in a.get('kpi_queries', {}).items():
        rows = q(sql)
        kpi[name] = (list(rows[0].values())[0] if len(rows) == 1 and len(rows[0]) == 1 else rows)

    alerts = []
    if a.get('alerts_query'):
        alerts += q(a['alerts_query'])
    if a.get('trend_query'):
        try:
            alerts += q(a['trend_query'])
        except Exception as e:
            print(f"WARNING trend_query: {e}")

    conn.close()
    return kpi, alerts


# ── Runner ATHENA (telecom) ─────────────────────────────────────────────────────
def run_athena(a, vars):
    athena = boto3.client('athena')
    db = a['database']
    out = a.get('athena_results') or f"s3://{a.get('results_bucket','')}/athena-results/"

    def q(sql):
        sql = _fill(sql, vars)
        qid = athena.start_query_execution(
            QueryString=sql, QueryExecutionContext={'Database': db},
            ResultConfiguration={'OutputLocation': out})['QueryExecutionId']
        while True:
            st = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
            if st['State'] not in ('QUEUED', 'RUNNING'):
                break
            time.sleep(1)
        if st['State'] != 'SUCCEEDED':
            raise RuntimeError(f"Athena {st['State']}: {st.get('StateChangeReason')} | SQL: {sql}")
        rs = athena.get_query_results(QueryExecutionId=qid)['ResultSet']['Rows']
        hdr = [c.get('VarCharValue') for c in rs[0]['Data']]
        return [dict(zip(hdr, [c.get('VarCharValue') for c in r['Data']])) for r in rs[1:]]

    kpi = {}
    for name, sql in a.get('kpi_queries', {}).items():
        rows = q(sql)
        kpi[name] = (list(rows[0].values())[0] if len(rows) == 1 and len(rows[0]) == 1 else rows)
    alerts = q(a['alerts_query']) if a.get('alerts_query') else []
    return kpi, alerts


# ── Runner CSV (legacy) ─────────────────────────────────────────────────────────
def run_csv(a, bucket):
    obj = s3.get_object(Bucket=bucket, Key=a['csv_source'])
    rows = list(csv.DictReader(io.StringIO(obj['Body'].read().decode('utf-8'))))
    kpi = {}
    for name, spec in a.get('aggregations', {}).items():
        op = spec['op']
        if op == 'sum':
            kpi[name] = round(sum(_num(r.get(spec['column'])) for r in rows), 2)
        elif op == 'count_distinct':
            kpi[name] = len({r.get(spec['column']) for r in rows})
        elif op == 'group_sum':
            g = defaultdict(lambda: defaultdict(float))
            for r in rows:
                for c in spec['columns']:
                    g[r.get(spec['group'])][c] += _num(r.get(c))
            kpi[name] = [dict(group=k, **{c: round(v[c], 1) for c in spec['columns']}) for k, v in g.items()]
    alerts = []
    al = a.get('alerts')
    if al and al.get('op') == 'worst_by_wape':
        acc = defaultdict(lambda: [0.0, 0.0])
        for r in rows:
            y, p = _num(r.get(al['actual'])), _num(r.get(al['pred']))
            acc[r.get(al['group'])][0] += abs(y - p)
            acc[r.get(al['group'])][1] += abs(y)
        wape = {k: (v[0] / v[1] * 100 if v[1] else 0.0) for k, v in acc.items()}
        for k, w in sorted(wape.items(), key=lambda kv: kv[1], reverse=True)[:al.get('top', 3)]:
            alerts.append({'entity': k, 'wape_pct': round(w, 1)})
    return kpi, alerts


# ── Cargador de FORECAST (genérico): agrupa los N días en semanas ──────────────
def load_forecast(a, bucket):
    fk = a.get('forecast_key')
    if not fk:
        return {}
    fa = a.get('forecast_agg', {})
    try:
        obj = s3.get_object(Bucket=bucket, Key=fk)
    except Exception as e:
        print(f"WARNING: no se pudo leer forecast {fk}: {e}")
        return {}
    rows = list(csv.DictReader(io.StringIO(obj['Body'].read().decode('utf-8'))))
    if not rows:
        return {}

    date_col = fa.get('date_col', 'fecha')
    valcol = fa.get('value', 'prediccion')
    weeks = int(fa.get('weeks', 4))
    seg_col = fa.get('by_segment')
    rel_col = fa.get('reliability')
    prop_keys = fa.get('by_property', ['property_id'])

    fechas = sorted({r.get(date_col) for r in rows if r.get(date_col)})
    week_of = {fe: min(i // 7 + 1, weeks) for i, fe in enumerate(fechas)}

    by_prop = defaultdict(lambda: defaultdict(float))
    by_seg = defaultdict(lambda: defaultdict(float))
    rel, label = {}, {}
    for r in rows:
        w = f"semana_{week_of.get(r.get(date_col), 1)}"
        pid = r.get(prop_keys[0])
        by_prop[pid][w] += _num(r.get(valcol))
        label[pid] = {k: r.get(k) for k in prop_keys}
        if seg_col:
            by_seg[r.get(seg_col)][w] += _num(r.get(valcol))
        if rel_col:
            rel[pid] = _num(r.get(rel_col))

    por_prop = [dict(**label[p], wape=round(rel.get(p, 0), 3),
                     **{w: round(by_prop[p][w], 1) for w in sorted(by_prop[p])}) for p in by_prop]
    por_seg = [dict(segmento=k, **{w: round(by_seg[k][w], 1) for w in sorted(by_seg[k])}) for k in by_seg]
    return {'por_propiedad': por_prop, 'por_segmento': por_seg}
