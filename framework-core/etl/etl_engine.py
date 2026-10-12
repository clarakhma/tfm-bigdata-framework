import sys
import io
import json
import argparse
from datetime import datetime, timezone

ENGINE_VERSION = "etl_engine/1.0.0"


# ==================================================================================
#  CONTEXTO — mantiene motor, sesión Spark, stats y config a lo largo de la receta
# ==================================================================================
class Ctx:
    def __init__(self, engine, config, spark=None):
        self.engine = engine            # 'pandas' | 'spark'
        self.config = config
        self.spark = spark
        self.stats = {}                 # bitácora: filas por paso, etc.

    def log(self, step, **kv):
        self.stats.setdefault("steps", []).append({"step": step, **kv})
        print(f"[{self.engine}] {step}: {kv}")


# ==================================================================================
#  ADAPTADOR — cada primitiva sabe hacerse en pandas Y en Spark (Wrapper Pattern)
# ==================================================================================
def _nrows(df, ctx):
    return df.count() if ctx.engine == "spark" else len(df)


def _files_for_window(base, pattern, ws, we):
    """Lista de rutas de archivos para la ventana [ws, we] (fechas ISO 'YYYY-MM-DD').
    pattern usa {date}: p.ej. 'sms-call-internet-mi-{date}.txt'."""
    from datetime import date, timedelta
    d0, d1 = date.fromisoformat(str(ws)), date.fromisoformat(str(we))
    base = base.rstrip("/")
    return [f"{base}/{pattern.format(date=(d0 + timedelta(i)).isoformat())}"
            for i in range((d1 - d0).days + 1)]


def op_read(ctx):
    """Ingesta ADAPTATIVA por esquema posicional (Config-as-Code). Usa el `data_contract`:
      file_format: { separator, has_header, recursive_lookup }
      positional_columns: [ {index, name, type}, ... ]
    - has_header=false → texto plano ciego: split por separador + .getItem(index).cast(type) (telecom TSV).
    - has_header=true  → lee con cabecera y castea por nombre según positional_columns (energía CSV).

    LECTURA INCREMENTAL (genérica, config-driven): si el YAML declara `data_contract.incremental`
    y el runtime trae `week_start`/`week_end`, se lee SOLO la ventana semanal:
      - mode: by_filename → lee solo los archivos de la semana (telecom; ahorra IO).
      - mode: by_column   → lee todo y filtra las filas por una columna de fecha (energía; un CSV).
    Sin bloque incremental o sin ventana → lee todo (compatibilidad hacia atrás)."""
    dc = ctx.config["data_contract"]
    fmt = dc.get("file_format", {})
    sep = fmt.get("separator", ",")
    has_header = fmt.get("has_header", True)
    recursive = str(fmt.get("recursive_lookup", True)).lower()
    cols = dc.get("positional_columns", [])
    src = ctx.config["etl"]["source"]["source"]

    inc = dc.get("incremental", {})
    win = ctx.config.get("runtime", {})
    ws, we = win.get("week_start"), win.get("week_end")
    src_read = src
    if inc.get("mode") == "by_filename" and ws and we:      # solo los archivos de la semana
        src_read = _files_for_window(src, inc["pattern"], ws, we)
        recursive = "false"
        ctx.log("incremental_by_filename", rows=len(src_read))

    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        if not has_header:                       # VÍA 1 — texto sin cabecera, mapeo por índice
            raw = ctx.spark.read.option("recursiveFileLookup", recursive).text(src_read)
            sc = F.split(F.col("value"), sep)
            exprs = [sc.getItem(c["index"]).cast(c["type"]).alias(c["name"]) for c in cols]
            df = raw.select(*exprs)
        else:                                    # VÍA 2 — con cabecera
            df = (ctx.spark.read.option("header", "true").option("delimiter", sep)
                  .option("recursiveFileLookup", recursive).csv(src_read))
            if cols:
                df = df.select(*[F.col(c["name"]).cast(c["type"]).alias(c["name"]) for c in cols])
        if inc.get("mode") == "by_column" and ws and we:     # filtra por columna de fecha
            dcol = inc["date_col"]
            df = df.filter((F.to_date(F.col(dcol)) >= F.lit(ws)) & (F.to_date(F.col(dcol)) <= F.lit(we)))
        return df
    else:  # pandas (energía, python_shell)
        import pandas as pd
        if not has_header:
            df = pd.read_csv(src, sep=sep, header=None, names=[c["name"] for c in cols])
        else:
            df = pd.read_csv(src, sep=sep)
            if cols:
                df = df[[c["name"] for c in cols]]
        for c in cols:                            # coerción de tipos según el YAML
            t = c["type"]
            if t == "timestamp":
                df[c["name"]] = pd.to_datetime(df[c["name"]], errors="coerce")
            elif t in ("int", "long", "double"):
                df[c["name"]] = pd.to_numeric(df[c["name"]], errors="coerce")
            else:
                df[c["name"]] = df[c["name"]].astype(str)
        if inc.get("mode") == "by_column" and ws and we:     # filtra las filas de la semana
            d = pd.to_datetime(df[inc["date_col"]], errors="coerce")
            df = df[(d >= pd.to_datetime(ws)) & (d < pd.to_datetime(we) + pd.Timedelta(days=1))]
        return df


def op_cast_types(ctx, df, params):
    """Coerce tipos. params: types: {col: 'int'|'double'|'string'|'timestamp'}."""
    types = params.get("types", {})
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        for c, t in types.items():
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast(t))
    else:
        import pandas as pd
        m = {"int": "Int64", "double": "float64", "string": "object"}
        for c, t in types.items():
            if c in df.columns:
                if t == "timestamp":
                    df[c] = pd.to_datetime(df[c], errors="coerce")
                else:
                    df[c] = pd.to_numeric(df[c], errors="coerce") if t in ("int", "double") else df[c].astype(str)
    return df


def op_normalize_time(ctx, df, params):
    """Crea la columna temporal base 'ds'. params: source_col, unit(ms/s/iso), target='ds', floor='hour'."""
    src = params["source_col"]
    tgt = params.get("target", "ds")
    unit = params.get("unit", "iso")
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        if unit == "ms":
            df = df.withColumn(tgt, F.to_timestamp((F.col(src).cast("long") / 1000)))
        elif unit == "s":
            df = df.withColumn(tgt, F.to_timestamp(F.col(src).cast("long")))
        else:
            df = df.withColumn(tgt, F.to_timestamp(F.col(src)))
        _fl = params.get("floor")
        if _fl in ("hour", "day"):
            df = df.withColumn(tgt, F.date_trunc(_fl, F.col(tgt)))
    else:
        import pandas as pd
        if unit == "ms":
            df[tgt] = pd.to_datetime(df[src].astype("int64"), unit="ms")
        elif unit == "s":
            df[tgt] = pd.to_datetime(df[src].astype("int64"), unit="s")
        else:
            df[tgt] = pd.to_datetime(df[src], errors="coerce")
        _fl = params.get("floor")
        if _fl == "hour":
            df[tgt] = df[tgt].dt.floor("h")
        elif _fl == "day":
            df[tgt] = df[tgt].dt.floor("D")
    return df


def op_drop_duplicates(ctx, df, params):
    keys = params.get("keys")
    return df.dropDuplicates(keys) if ctx.engine == "spark" else df.drop_duplicates(subset=keys)


def op_drop_nulls(ctx, df, params):
    """Elimina filas con NULL en columnas CLAVE (estructural, NO imputación estadística)."""
    cols = params.get("columns")
    return df.dropna(subset=cols) if ctx.engine == "spark" else df.dropna(subset=cols)


def op_fill_zeros(ctx, df, params):
    """Regla de negocio: NULL = 'no hubo actividad' → 0. params: columns: [...]. (NO es imputación ML.)"""
    cols = params.get("columns", [])
    if ctx.engine == "spark":
        return df.fillna(0, subset=cols)
    else:
        for c in cols:
            if c in df.columns:
                df[c] = df[c].fillna(0)
        return df


def op_derive_columns(ctx, df, params):
    """Expression Evaluator: columnas derivadas declaradas como fórmulas de texto en el YAML.
    params: columns: { nombre: 'expresion' }  (ej. total_activity: 'sms_in + sms_out + internet')."""
    exprs = params.get("columns", {})
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        for name, expr in exprs.items():
            df = df.withColumn(name, F.expr(expr))   # evalúa SQL de Spark
    else:
        for name, expr in exprs.items():
            df[name] = df.eval(expr)                  # evalúa expresión pandas
    return df


def op_datetime_features(ctx, df, params):
    """Features de calendario deterministas (NO filtran: salen solo de la propia fila).
    params: source='ds', features: [hour, day_of_week, month, is_weekend]."""
    src = params.get("source", "ds")
    feats = params.get("features", ["hour", "day_of_week", "is_weekend"])
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        if "hour" in feats:        df = df.withColumn("hour", F.hour(src))
        if "day_of_week" in feats: df = df.withColumn("day_of_week", F.dayofweek(src))
        if "month" in feats:       df = df.withColumn("month", F.month(src))
        if "week_of_year" in feats: df = df.withColumn("week_of_year", F.weekofyear(src))
        if "is_weekend" in feats:  df = df.withColumn("is_weekend",
                                        F.when(F.dayofweek(src).isin(1, 7), 1).otherwise(0))
        if "periodo" in feats:     df = df.withColumn("periodo", F.date_format(src, "yyyy-MM"))
    else:
        if "hour" in feats:        df["hour"] = df[src].dt.hour
        if "day_of_week" in feats: df["day_of_week"] = df[src].dt.dayofweek
        if "month" in feats:       df["month"] = df[src].dt.month
        if "week_of_year" in feats: df["week_of_year"] = df[src].dt.isocalendar().week.astype("int64")
        if "is_weekend" in feats:  df["is_weekend"] = (df[src].dt.dayofweek >= 5).astype(int)
        if "periodo" in feats:     df["periodo"] = df[src].dt.strftime("%Y-%m")
    return df


def op_aggregate(ctx, df, params):
    """Agrega. params: group_by: [...], aggregations: { out: 'func(col)' } (func: sum/avg/max/min)."""
    keys = params["group_by"]
    aggs = params.get("aggregations", {})
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        agg_exprs = [getattr(F, fn.split("(")[0])(fn[fn.find("(") + 1:-1]).alias(out)
                     for out, fn in aggs.items()]
        return df.groupBy(*keys).agg(*agg_exprs)
    else:
        spec = {fn[fn.find("(") + 1:-1]: ("mean" if fn.startswith("avg") else fn.split("(")[0])
                for out, fn in aggs.items()}
        g = df.groupby(keys).agg(spec).reset_index()
        g.columns = keys + list(aggs.keys())
        return g


def op_add_lags(ctx, df, params):
    """Features de retardo (lag) por entidad, ordenadas en el tiempo.
    params: partition_by, order_by, lags: { out_col: {column, periods} }.
    Nota: un lag es un valor pasado determinista (no filtra el target); se declara aquí por
    conveniencia, pero la imputación ESTADÍSTica sí debe ir al modelo (Data Leakage)."""
    pb = params["partition_by"]
    ob = params["order_by"]
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        w = Window.partitionBy(pb).orderBy(ob)
        for out, spec in params["lags"].items():
            df = df.withColumn(out, F.lag(spec["column"], spec["periods"]).over(w))
    else:
        df = df.sort_values(ob)
        for out, spec in params["lags"].items():
            df[out] = df.groupby(pb)[spec["column"]].shift(spec["periods"])
    return df


def _read_csv_pandas(uri):
    """Lee un CSV de referencia (S3 o local) como DataFrame de pandas."""
    import pandas as pd
    if uri.startswith("s3://"):
        import boto3
        _, _, rest = uri.partition("s3://")
        bucket, _, key = rest.partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return pd.read_csv(io.BytesIO(body))
    return pd.read_csv(uri)


def op_flag_and_fill_null(ctx, df, params):
    """Marca los nulos de una columna en un flag y los rellena con un valor fijo.
    params: column, flag_col(opcional), fill_value.
    Ej: method NULL = 'lectura real' → is_real_reading=1 + method='real' (regla de negocio, NO imputación ML)."""
    col = params["column"]
    flag = params.get("flag_col")
    val = params.get("fill_value")
    # OJO: al castear a string, un NaN real se vuelve la CADENA 'nan'  -> isNull() no lo ve.
    # Tratamos como "vacío" tanto el null real como esas cadenas (bug detectado en el EDA).
    _empty = ("nan", "none", "", "<na>", "null")
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        es_vacio = F.col(col).isNull() | F.lower(F.trim(F.col(col).cast("string"))).isin(list(_empty))
        if flag:
            df = df.withColumn(flag, F.when(es_vacio, 1).otherwise(0))
        df = df.withColumn(col, F.when(es_vacio, F.lit(val)).otherwise(F.col(col)))
    else:
        es_vacio = df[col].isna() | df[col].astype(str).str.strip().str.lower().isin(_empty)
        if flag:
            df[flag] = es_vacio.astype(int)
        df.loc[es_vacio, col] = val
    return df


def op_group_flag(ctx, df, params):
    """Etiqueta categórica derivada de un agregado por grupo frente a un umbral (regla determinista).
    params: group_by, agg_col, agg(max|min|mean), threshold, out_col, labels:{above, below_or_equal}.
    Ej: tariff_type = '3.0/6.1' si max(period) por propiedad > 3, si no '2.0' (D48)."""
    gb = params["group_by"]; ac = params["agg_col"]; agg = params.get("agg", "max")
    thr = params["threshold"]; out = params["out_col"]
    above = params["labels"]["above"]; below = params["labels"]["below_or_equal"]
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        agv = getattr(F, agg)(F.col(ac)).over(Window.partitionBy(gb))
        df = df.withColumn(out, F.when(agv > thr, F.lit(above)).otherwise(F.lit(below)))
    else:
        import numpy as np
        gv = df.groupby(gb)[ac].transform(agg)
        df[out] = np.where(gv > thr, above, below)
    return df


def op_flag_outliers(ctx, df, params):
    """Marca outliers por grupo con la regla robusta Q3 + factor·IQR (NO los elimina — evidencia de calidad).
    params: group_by, column, factor(3.0), out_col('is_outlier')."""
    gb = params["group_by"]; col = params["column"]; factor = float(params.get("factor", 3.0))
    out = params.get("out_col", "is_outlier")
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        stats = df.groupBy(gb).agg(
            F.expr(f"percentile_approx({col}, 0.25)").alias("_q1"),
            F.expr(f"percentile_approx({col}, 0.75)").alias("_q3"))
        df = df.join(stats, gb, "left")
        lim = F.col("_q3") + factor * (F.col("_q3") - F.col("_q1"))
        df = df.withColumn(out, F.when(F.col(col) > lim, 1).otherwise(0)).drop("_q1", "_q3")
    else:
        q1 = df.groupby(gb)[col].transform(lambda s: s.quantile(0.25))
        q3 = df.groupby(gb)[col].transform(lambda s: s.quantile(0.75))
        lim = q3 + factor * (q3 - q1)
        df[out] = ((df[col] > lim) & df[col].notna()).astype(int)
    return df


def op_add_holiday_flag(ctx, df, params):
    """is_holiday=1 si la fecha (día) es festivo. Determinista, se conoce por calendario (NO leak).
    params: source='ds', out_col='is_holiday', holidays:[...] (lista) o holidays_file + date_col."""
    src = params.get("source", "ds"); out = params.get("out_col", "is_holiday")
    fechas = set(str(d) for d in params.get("holidays", []))
    hf = params.get("holidays_file"); date_col = params.get("date_col", "date")
    if hf and not fechas:
        import pandas as pd
        ref = _read_csv_pandas(hf)
        fechas = set(pd.to_datetime(ref[date_col]).dt.strftime("%Y-%m-%d"))
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        df = df.withColumn(out, F.when(
            F.date_format(F.col(src), "yyyy-MM-dd").isin(list(fechas)), 1).otherwise(0))
    else:
        import pandas as pd
        d = pd.to_datetime(df[src]).dt.strftime("%Y-%m-%d")
        df[out] = d.isin(fechas).astype(int)
    return df


def op_join_reference(ctx, df, params):
    """LEFT JOIN con un CSV de referencia externo (variable exógena; ej. temperatura por fecha).
    params: ref_source(uri), left_on, right_on, columns:[...], as_date(bool: unir por día)."""
    ref_uri = params["ref_source"]; left_on = params["left_on"]; right_on = params["right_on"]
    cols = params.get("columns", []); as_date = params.get("as_date", False)
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        ref = ctx.spark.read.option("header", "true").option("inferSchema", "true").csv(ref_uri)
        if as_date:
            df = df.withColumn("_ljk", F.to_date(F.col(left_on)))
            ref = ref.withColumn("_rjk", F.to_date(F.col(right_on)))
        else:
            df = df.withColumn("_ljk", F.col(left_on)); ref = ref.withColumn("_rjk", F.col(right_on))
        ref = ref.select(["_rjk"] + [c for c in cols if c in ref.columns])
        return df.join(ref, df["_ljk"] == ref["_rjk"], "left").drop("_ljk", "_rjk")
    else:
        import pandas as pd
        ref = _read_csv_pandas(ref_uri)
        keep = [right_on] + [c for c in cols if c in ref.columns]
        ref = ref[keep].copy()
        if as_date:
            df["_ljk"] = pd.to_datetime(df[left_on]).dt.strftime("%Y-%m-%d")
            ref["_rjk"] = pd.to_datetime(ref[right_on]).dt.strftime("%Y-%m-%d")
        else:
            df["_ljk"] = df[left_on]; ref["_rjk"] = ref[right_on]
        ref = ref.drop(columns=[right_on])
        return df.merge(ref, left_on="_ljk", right_on="_rjk", how="left").drop(columns=["_ljk", "_rjk"])


def op_string_shorten(ctx, df, params):
    """Acorta un identificador largo a 'PREFIJO<sep>SUFIJO' (legibilidad en dashboards/tablas).
    params: column, prefix(2), suffix(2), sep('-'). Ej UUID c11bc986-...-448e311d8b8e → c1-8e (D47)."""
    col = params["column"]; p = int(params.get("prefix", 2)); s = int(params.get("suffix", 2))
    sep = params.get("sep", "-")
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        df = df.withColumn(col, F.expr(
            f"concat(substring({col},1,{p}),'{sep}',substring({col},length({col})-{s}+1,{s}))"))
    else:
        df[col] = df[col].astype(str).apply(lambda x: f"{x[:p]}{sep}{x[-s:]}" if len(x) > p + s else x)
    return df


def op_write_parquet(ctx, df, params):
    """Escribe Parquet. params: target, partition_by(list, opcional)."""
    tgt = params["target"]
    part = params.get("partition_by") or []
    if ctx.engine == "spark":
        w = df.write.mode(params.get("mode", "overwrite"))
        if part:
            ctx.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            w = w.partitionBy(*part)
        w.parquet(tgt)
    else:
        import os
        import pandas as pd  # noqa
        if tgt.startswith("s3://"):
            # Escribe local y sube con boto3 → NO necesita s3fs (evita PyPI/NAT en la VPC).
            import boto3, glob
            _, _, rest = tgt.partition("s3://")
            bucket, _, prefix = rest.partition("/")
            prefix = prefix.rstrip("/")
            s3 = boto3.client("s3")
            if part:
                local = "/tmp/_pq_out"
                df.to_parquet(local, index=False, partition_cols=part)
                for fp in glob.glob(local + "/**/*.parquet", recursive=True):
                    key = prefix + "/" + os.path.relpath(fp, local).replace(os.sep, "/")
                    s3.upload_file(fp, bucket, key)
            else:
                local = "/tmp/data.parquet"
                df.to_parquet(local, index=False)
                s3.upload_file(local, bucket, prefix + "/data.parquet")
        else:                                                # local (test): crear carpeta
            single = tgt.rstrip("/") + "/data.parquet"
            os.makedirs(tgt.rstrip("/") if part else os.path.dirname(single), exist_ok=True)
            if part:
                df.to_parquet(tgt, index=False, partition_cols=part)
            else:
                df.to_parquet(single, index=False)
    return df


def load_rds_atomic(df, storage, table):
    """Carga un DataFrame en una tabla RDS Postgres con REEMPLAZO TOTAL ATÓMICO.
    TRUNCATE + INSERT dentro de UNA transacción: los lectores nunca ven la tabla
    vacía (siguen con los datos viejos hasta el COMMIT) → sin hueco en el dashboard.
    Credenciales desde Secrets Manager (storage.secret_name)."""
    import boto3
    import psycopg2
    from psycopg2.extras import execute_values

    sec = json.loads(boto3.client("secretsmanager")
                     .get_secret_value(SecretId=storage["secret_name"])["SecretString"])
    conn = psycopg2.connect(
        host=sec["host"],
        port=int(sec.get("port", storage.get("port", 5432))),
        dbname=storage.get("database") or sec.get("dbname"),
        user=sec["username"], password=sec["password"])
    try:
        cols = list(df.columns)
        collist = ",".join(f'"{c}"' for c in cols)
        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        with conn:                                   # transacción: COMMIT al salir, ROLLBACK si falla
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE {table}')
                execute_values(cur, f'INSERT INTO {table} ({collist}) VALUES %s',
                               rows, page_size=1000)
    finally:
        conn.close()
    return len(df)


def op_sink_analytics(ctx, df, params):
    """Carga la sábana a la capa analítica declarada en `storage.analytical`:
      rds    = COPY atómico a RDS (energía → QuickSight/SPICE + KPIs del informe)
      athena = NO hace nada (el parquet particionado en S3 YA es la tabla)
    Genérico: la diferencia vive en el YAML, no en el código."""
    st = ctx.config.get("storage", {})
    analytical = st.get("analytical")
    if analytical == "rds":
        pdf = df.toPandas() if ctx.engine == "spark" else df   # el load usa pandas
        table = params.get("table") or st["dashboard_table"]
        n = load_rds_atomic(pdf, st, table)
        ctx.log("sink_analytics", engine="rds", table=table, rows=n)
    else:
        ctx.log("sink_analytics", engine=analytical or "none", note="sin carga (athena lee S3)")
    return df


def op_add_rolling(ctx, df, params):
    """Features móviles RETROSPECTIVAS por entidad (sin leakage: shift(k) excluye el bloque a predecir).
    params: partition_by, order_by, shift (int, default 1), min_periods (int, default 1),
            features: { out_col: {column, window|expanding, agg} }  agg: mean|std."""
    pb = params["partition_by"]; ob = params["order_by"]
    sh = int(params.get("shift", 1)); mp = int(params.get("min_periods", 1))
    feats = params["features"]
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        base = Window.partitionBy(*pb).orderBy(ob)
        for out, spec in feats.items():
            col = spec["column"]; agg = spec.get("agg", "mean")
            if spec.get("expanding"):
                w = base.rowsBetween(Window.unboundedPreceding, -sh)
            else:
                n = int(spec["window"]); w = base.rowsBetween(-(sh + n - 1), -sh)
            fn = F.avg if agg == "mean" else F.stddev
            df = df.withColumn(out, fn(F.col(col)).over(w))
        return df
    else:
        df = df.sort_values(pb + [ob])
        for out, spec in feats.items():
            col = spec["column"]; agg = spec.get("agg", "mean")
            g = df.groupby(pb)[col]
            if spec.get("expanding"):
                df[out] = g.transform(lambda x: getattr(x.shift(sh).expanding(min_periods=mp), agg)())
            else:
                n = int(spec["window"])
                df[out] = g.transform(lambda x: getattr(x.shift(sh).rolling(n, min_periods=mp), agg)())
        return df


def op_fill_missing_days(ctx, df, params):
    """Rellena huecos de calendario por entidad para tener una serie diaria CONTINUA antes de los lags.
    Sin leakage cruzando el split porque los huecos caen en train. Reglas por longitud del hueco:
      - <= max_interp_gap días (hasta una semana) → interpolación lineal entre el día previo y el posterior.
      - > max_interp_gap días                     → media del MISMO día de semana de las últimas dow_weeks
                                                    semanas; fallback a la media de la entidad.
    params: partition_by, order_by(=ds), value_col(=y), max_interp_gap(=7), dow_weeks(=4). Solo pandas."""
    import pandas as pd
    if ctx.engine == "spark":
        raise NotImplementedError("fill_missing_days solo está implementado para pandas (energía).")
    pb = params["partition_by"]; ob = params.get("order_by", "ds"); vc = params.get("value_col", "y")
    max_interp = int(params.get("max_interp_gap", 7)); dow_weeks = int(params.get("dow_weeks", 4))
    piezas = []
    for keys, g in df.groupby(pb):
        g = g.sort_values(ob).copy()
        g[ob] = pd.to_datetime(g[ob])
        full = pd.date_range(g[ob].min(), g[ob].max(), freq="D")   # calendario diario completo de la entidad
        g = g.set_index(ob).reindex(full)
        g.index.name = ob
        na = g[vc].isna()
        run_len = na.groupby((~na).cumsum()).transform("sum")       # longitud de cada racha de NaN
        # 1) huecos cortos → interpolación lineal (solo entre valores conocidos, sin extrapolar bordes)
        interp = g[vc].interpolate(method="linear", limit_area="inside")
        cortos = na & (run_len <= max_interp)
        g.loc[cortos, vc] = interp[cortos]
        # 2) huecos largos → media del mismo día de semana de las dow_weeks semanas previas
        largos = g[vc].isna()
        if largos.any():
            s = g[vc]; dow = g.index.dayofweek; media_ent = s.mean()
            for ts in g.index[largos]:
                prev = s[(g.index < ts) & (dow == ts.dayofweek)].dropna().tail(dow_weeks)
                g.loc[ts, vc] = prev.mean() if len(prev) else media_ent
        g = g.reset_index()
        if not isinstance(keys, tuple):
            keys = (keys,)
        for c, v in zip(pb, keys):
            g[c] = v
        for c in df.columns:                                        # propaga columnas constantes por entidad (tariff_type)
            if c in pb or c in (ob, vc):
                continue
            g[c] = g[c].ffill().bfill()
        piezas.append(g)
    return pd.concat(piezas, ignore_index=True)


# REGISTRO MAESTRO: nombre del paso en el YAML → función Python (Command Pattern) 
STEP_REGISTRY = {
    "cast_types": op_cast_types,
    "normalize_time": op_normalize_time,
    "drop_duplicates": op_drop_duplicates,
    "drop_nulls": op_drop_nulls,
    "fill_zeros": op_fill_zeros,
    "flag_and_fill_null": op_flag_and_fill_null,
    "datetime_features": op_datetime_features,
    "derive_columns": op_derive_columns,
    "aggregate": op_aggregate,
    "group_flag": op_group_flag,
    "flag_outliers": op_flag_outliers,
    "add_holiday_flag": op_add_holiday_flag,
    "join_reference": op_join_reference,
    "add_lags": op_add_lags,
    "add_rolling": op_add_rolling,
    "fill_missing_days": op_fill_missing_days,
    "string_shorten": op_string_shorten,
    "write_parquet": op_write_parquet,
    "sink_analytics": op_sink_analytics,
}


# ==================================================================================
#  INTÉRPRETE — ejecuta la receta declarada en el YAML
# ==================================================================================
def run_recipe(ctx, df, steps):
    for st in steps:
        name = st["step"]
        if name not in STEP_REGISTRY:
            raise ValueError(f"Paso '{name}' declarado en el YAML no existe en STEP_REGISTRY.")
        df = STEP_REGISTRY[name](ctx, df, st.get("params", {}))
        ctx.log(name, rows=_nrows(df, ctx))
    return df


# ==================================================================================
#  CALIDAD DE DATOS — informe rico (perfil + bitácora + señales), genérico y por motor
# ==================================================================================
def _nulls_by_col(ctx, df):
    """Nulos por columna (una sola pasada). Responde: '¿cuántos nulos había en cada columna?'"""
    if ctx.engine == "spark":
        from pyspark.sql import functions as F
        row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
        return {c: int(row[c] or 0) for c in df.columns}
    return {c: int(df[c].isna().sum()) for c in df.columns}


def profile_df(ctx, df, id_col=None, time_col=None, value_col=None):
    """Fotografía de un DataFrame: filas, columnas, nulos por columna, entidades únicas,
    rango de fechas y % de valor en cero. Evidencia de calidad para la defensa."""
    rows = _nrows(df, ctx)
    cols = list(df.columns)
    nulos = _nulls_by_col(ctx, df)
    total = rows * len(cols)
    nulas = sum(nulos.values())
    prof = {
        "filas": rows,
        "columnas": len(cols),
        "nombres_columnas": cols,
        "celdas_totales": total,
        "celdas_nulas": nulas,
        "pct_celdas_nulas": round(nulas / max(total, 1) * 100, 2),
        "nulos_por_columna": nulos,
    }
    if id_col and id_col in cols:
        prof["entidades_unicas"] = (df.select(id_col).distinct().count()
                                    if ctx.engine == "spark" else int(df[id_col].nunique()))
    if time_col and time_col in cols:
        if ctx.engine == "spark":
            from pyspark.sql import functions as F
            r = df.select(F.min(time_col).alias("mn"), F.max(time_col).alias("mx")).collect()[0]
            prof["fecha_min"], prof["fecha_max"] = str(r["mn"]), str(r["mx"])
        else:
            prof["fecha_min"], prof["fecha_max"] = str(df[time_col].min()), str(df[time_col].max())
    if value_col and value_col in cols:
        if ctx.engine == "spark":
            from pyspark.sql import functions as F
            z = df.filter(F.col(value_col) == 0).count()
        else:
            z = int((df[value_col] == 0).sum())
        prof["pct_valor_cero"] = round(z / max(rows, 1) * 100, 2)
    return prof


def quality_signals(ctx, df):
    """Señales de calidad si el paso las dejó como columna (genérico: solo si existe la columna)."""
    sig = {}

    def col_sum(c):
        if ctx.engine == "spark":
            from pyspark.sql import functions as F
            return int(df.agg(F.sum(F.col(c))).collect()[0][0] or 0)
        return int(df[c].sum())

    if "is_outlier" in df.columns:      sig["outliers_marcados"] = col_sum("is_outlier")
    if "is_real_reading" in df.columns: sig["lecturas_reales"] = col_sum("is_real_reading")
    if "is_holiday" in df.columns:      sig["filas_festivo"] = col_sum("is_holiday")
    return sig


def constant_cols(ctx, df):
    """Columnas de varianza cero (sin capacidad predictiva). En Spark se omite (costoso)."""
    if ctx.engine == "spark":
        return []
    return [c for c in df.columns if df[c].nunique(dropna=False) <= 1]


def save_quality_report(report, quality_uri, sector):
    """Escribe UN informe de calidad (processed + ml_input) con el nombre de siempre:
    quality/{sector}_quality_{fecha}.json."""
    fecha = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fname = f"{sector}_quality_{fecha}.json"
    body = json.dumps(report, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    if quality_uri.startswith("s3://"):
        import boto3
        _, _, rest = quality_uri.partition("s3://")
        bucket, _, prefix = rest.rstrip("/").partition("/")
        key = f"{prefix}/{fname}" if prefix else fname
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)
        print(f"Quality → s3://{bucket}/{key}")
    else:
        with open(fname, "wb") as f:
            f.write(body)
        print(f"Quality → {fname}")


# ==================================================================================
#  MAIN — carga YAML, elige motor, corre la receta ML (y opcionalmente EDA)
# ==================================================================================
def load_config(uri):
    import yaml
    if uri.startswith("s3://"):
        import boto3
        _, _, rest = uri.partition("s3://")
        bucket, _, key = rest.partition("/")
        raw = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        raw = open(uri, "rb").read()
    return yaml.safe_load(raw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="ruta/URI del YAML del sector")
    ap.add_argument("--recipe", default="both", help="both | ml_steps | eda_steps")
    ap.add_argument("--week-start", default=None, help="incremental: inicio de la ventana (YYYY-MM-DD)")
    ap.add_argument("--week-end", default=None, help="incremental: fin de la ventana (YYYY-MM-DD)")
    args, _ = ap.parse_known_args()

    config = load_config(args.config)
    if args.week_start and args.week_end:                       # inyecta la ventana → op_read la usa
        config.setdefault("runtime", {}).update(week_start=args.week_start, week_end=args.week_end)
    etl = config["etl"]
    engine = "spark" if config["pipeline"]["glue_job_type"] == "pyspark" else "pandas"

    spark = None
    if engine == "spark":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.adaptive.enabled", "true")

    ctx = Ctx(engine, config, spark)

    dc = config["data_contract"]
    id_col = dc.get("id_column")
    vals = dc.get("value_columns") or [dc.get("value_column")]
    value_col = vals[0] if vals else None

    # 1) Lectura ÚNICA del crudo + perfil de entrada (compartido por ambas recetas).
    df_raw = op_read(ctx)
    ctx.log("read", rows=_nrows(df_raw, ctx))
    input_profile = profile_df(ctx, df_raw, id_col=id_col, value_col=value_col)

    # 2) Corre las recetas pedidas (por defecto AMBAS: eda→processed/ y ml→ml_input/),
    #    captura una SECCIÓN de calidad por cada una. Cada receta incluye su write_parquet.
    name_map = {"eda_steps": "processed", "ml_steps": "ml_input"}
    to_run = ["eda_steps", "ml_steps"] if args.recipe == "both" else [args.recipe]
    secciones = {}
    for rc in to_run:
        ctx.stats["steps"] = []                                  # bitácora limpia por sección
        df = df_raw if ctx.engine == "spark" else df_raw.copy()  # no mutar el crudo
        df = run_recipe(ctx, df, etl[rc])
        vc = value_col if (value_col and value_col in df.columns) else ("y" if "y" in df.columns else None)
        secciones[name_map.get(rc, rc)] = {
            "receta": rc,
            "pasos": list(ctx.stats.get("steps", [])),           # filas tras cada paso (dedup, drops, agregación)
            "senales_calidad": quality_signals(ctx, df),
            "columnas_constantes": constant_cols(ctx, df),
            "salida": profile_df(ctx, df, id_col=id_col, time_col="ds", value_col=vc),
        }

    # 3) UN informe de calidad con las dos secciones, nombre de siempre {sector}_quality_{fecha}.json.
    report = {
        "engine_version": ENGINE_VERSION,
        "utc_timestamp": datetime.now(timezone.utc).isoformat(),
        "fecha_ejecucion": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "sector": config.get("sector"),
        "compute_engine": engine,
        "input": input_profile,
        **secciones,
        "status": "OK",
    }
    quality_uri = config.get("governance", {}).get("quality_uri",
                  config["bucket"].rstrip("/") + "/quality/")
    save_quality_report(report, quality_uri, config.get("sector", "sector"))
    print("ETL declarativo COMPLETO (processed + ml_input + quality en un solo informe).")


if __name__ == "__main__":
    main()
