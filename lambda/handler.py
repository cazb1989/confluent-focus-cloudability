"""
Lambda: Confluent Cloud Billing API -> FOCUS 1.1 -> S3 (Cloudability FOCUS Ingress).

Corre una vez al día (EventBridge). En cada ejecución:
  - Regenera el mes en curso completo hasta la fecha (lo que exige Cloudability).
  - Durante los primeros días del mes también regenera el mes anterior, porque
    la API de Confluent publica costos con hasta 72 h de retraso.
  - Escribe cada mes en una carpeta epoch nueva; Cloudability lee la más reciente.

Variables de entorno:
  SECRET_ARN      Secret JSON {"api_key": "...", "api_secret": "..."}
  BUCKET          Bucket destino (= Root Directory en Cloudability)
  SUB_DIR         Subcarpeta dentro del bucket (p.ej. finops-demo)
  PREFIX          Manifest Prefix (p.ej. confluent-focus)
  ORG_ID          ID de la organización Confluent
  ORG_NAME        Nombre a mostrar del billing account
  LOOKBACK_DAYS   Días hacia atrás para cubrir la latencia (default 4)
"""
import json, os, shutil, tempfile, urllib.error
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import boto3
import confluent_to_focus as c2f

s3 = boto3.client("s3")
sm = boto3.client("secretsmanager")


def _load_credentials():
    sec = json.loads(sm.get_secret_value(SecretId=os.environ["SECRET_ARN"])["SecretString"])
    os.environ["CONFLUENT_API_KEY"] = sec["api_key"]
    os.environ["CONFLUENT_API_SECRET"] = sec["api_secret"]
    return sec["api_key"], sec["api_secret"]


def _env_names(key, secret):
    """Nombres de environments para SubAccountName (si falla, sigue sin nombres)."""
    try:
        url, names = "https://api.confluent.cloud/org/v2/environments?page_size=100", {}
        while url:
            page = c2f._get(url, key, secret)
            for e in page.get("data") or []:
                names[e["id"]] = e.get("display_name", "")
            url = (page.get("metadata") or {}).get("next") or None
        return names
    except Exception as exc:  # noqa: BLE001
        print(f"WARN no se pudieron leer environments: {exc}")
        return {}


def months_to_refresh(today: date, lookback: int):
    """Meses cuyo drop hay que regenerar hoy."""
    months = {(today - timedelta(days=1)).replace(day=1)}
    months.add((today - timedelta(days=lookback)).replace(day=1))
    return sorted(months)


def _upload_dir(local_root, bucket):
    keys = []
    for root, _, files in os.walk(local_root):
        # manifest al final: Cloudability no debe ver un manifest sin su data
        for f in sorted(files, key=lambda n: n.endswith("-Manifest.json")):
            path = os.path.join(root, f)
            key = os.path.relpath(path, local_root).replace(os.sep, "/")
            ctype = "application/json" if f.endswith(".json") else "application/gzip"
            s3.upload_file(path, bucket, key, ExtraArgs={"ContentType": ctype})
            keys.append(key)
    return keys


RAW_COLUMNS = ["start_date", "end_date", "granularity", "product", "line_type", "network_access_type",
               "resource_id", "resource_name", "environment_id", "environment_name",
               "price", "unit", "quantity", "original_amount", "discount_amount", "amount",
               "currency", "organization_id"]


def flatten(item, env_names, org_id):
    """Fila 'cruda' aplanada para el FOCUS Agent (sin convertir a FOCUS)."""
    res = item.get("resource") or {}
    env = (res.get("environment") or {}).get("id") or ""
    line = item.get("line_type") or ""
    credit = "CREDIT" in line.upper() or "PROMO" in line.upper()
    return {
        "start_date": item.get("start_date", ""), "end_date": item.get("end_date", ""),
        "granularity": item.get("granularity", ""),
        # las líneas de crédito vienen sin product: se rellena para que ServiceName no quede vacío
        "product": item.get("product") or ("CREDIT" if credit else "OTHER"),
        "line_type": line, "network_access_type": item.get("network_access_type", ""),
        "resource_id": res.get("id", ""), "resource_name": res.get("display_name", ""),
        "environment_id": env, "environment_name": env_names.get(env, ""),
        "price": item.get("price", ""), "unit": item.get("unit", ""),
        "quantity": item.get("quantity", ""), "original_amount": item.get("original_amount", ""),
        "discount_amount": item.get("discount_amount", 0), "amount": item.get("amount", ""),
        "currency": "USD", "organization_id": org_id,
    }


def write_agent_file(bucket, key, items, env_names, org_id):
    import csv, io
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=RAW_COLUMNS, lineterminator="\n")
    w.writeheader()
    rows = sorted((flatten(i, env_names, org_id) for i in items),
                  key=lambda r: (r["start_date"], r["product"], r["resource_id"], r["line_type"]))
    w.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode(), ContentType="text/csv")
    return len(rows)


def lambda_handler(event, context):
    event = event or {}
    bucket = os.environ["BUCKET"]
    sub_dir = os.environ.get("SUB_DIR", "finops-demo")
    prefix = os.environ.get("PREFIX", "confluent-focus")
    org_id = os.environ["ORG_ID"]
    org_name = os.environ.get("ORG_NAME", "Confluent Cloud Organization")
    lookback = int(os.environ.get("LOOKBACK_DAYS", "4"))

    today = date.fromisoformat(event["today"]) if event.get("today") else datetime.now(timezone.utc).date()
    # Permite forzar meses: {"months": ["2026-10"]}
    months = ([date.fromisoformat(m + "-01") for m in event["months"]]
              if event.get("months") else months_to_refresh(today, lookback))

    key, secret = _load_credentials()
    env_names = _env_names(key, secret)
    epoch = int(datetime.now(timezone.utc).timestamp())
    out = tempfile.mkdtemp(dir="/tmp")
    summary, agent_items = [], []
    try:
        for m in months:
            end = min((m + timedelta(days=32)).replace(day=1), today)
            if end <= m:
                continue
            try:
                items = c2f.fetch_live(m, end)
            except urllib.error.HTTPError as e:
                raise RuntimeError(f"Confluent API {e.code} para {m:%Y-%m}: {e.read()[:300]!r}") from e
            agent_items += items
            rows =[c2f.to_focus(i, org_id, org_name, env_names, "USD") for i in items]
            if not rows:
                summary.append({"month": f"{m:%Y-%m}", "rows": 0, "note": "sin costos aún"})
                continue
            errs = c2f.validate(rows)
            if errs:
                raise ValueError(f"Validación FOCUS falló {m:%Y-%m}: {errs[:10]}")
            rel = c2f.write_drop(out, bucket, sub_dir, prefix, m, rows, epoch)
            summary.append({"month": f"{m:%Y-%m}", "rows": len(rows), "path": rel,
                            "billed": round(sum(float(r["BilledCost"]) for r in rows), 4),
                            "list_usage": round(sum(float(r["ListCost"]) for r in rows
                                                    if r["ChargeCategory"] == "Usage"), 4)})
        uploaded = _upload_dir(out, bucket)
    finally:
        shutil.rmtree(out, ignore_errors=True)
    # Archivo de nombre fijo para el FOCUS Agent (Cloud Storage -> exact filename)
    agent_key = os.environ.get("AGENT_KEY", "focus-agent/confluent_costs_mtd.csv")
    agent_rows = write_agent_file(bucket, agent_key, agent_items, env_names, org_id)
    result = {"today": str(today), "months": summary, "uploaded": uploaded,
              "focus_agent_file": {"key": agent_key, "rows": agent_rows}}
    print(json.dumps(result))
    return result
