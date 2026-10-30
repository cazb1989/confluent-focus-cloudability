#!/usr/bin/env python3
"""
Confluent Cloud Billing API (/billing/v1/costs)  ->  FOCUS 1.1  ->  drop para
IBM Cloudability "Connect Custom Data (FOCUS Ingress)".

Dos modos de entrada:
  --live        Llama a la API (pagina y parte el rango en ventanas de <=1 mes).
                Credenciales por variables de entorno:
                  CONFLUENT_API_KEY / CONFLUENT_API_SECRET   (Cloud API key, rol BillingAdmin u OrganizationAdmin)
  --input F...  Lee respuestas JSON guardadas (p.ej. desde Bruno: Response -> Save).

Salida (estructura exigida por Cloudability):
  <out>/<sub_dir>/Confluent/<YYYYMMDD-YYYYMMDD>/<epoch>/<prefix>-00001.csv.gz
  <out>/<sub_dir>/Confluent/<YYYYMMDD-YYYYMMDD>/<epoch>/<prefix>-Manifest.json

Ejemplos:
  export CONFLUENT_API_KEY=...; export CONFLUENT_API_SECRET=...
  python3 confluent_to_focus.py --live --start 2026-10-01 --end 2026-11-01 \
      --root-dir mi-bucket --org-id <CONFLUENT_ORG_ID> \
      --env-names env-abc123=produccion env-def456=desarrollo

  python3 confluent_to_focus.py --input costs_oct.json --root-dir mi-bucket --org-id <org>
"""
import argparse, base64, csv, gzip, hashlib, io, json, os, sys, urllib.parse, urllib.request, uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

VENDOR = "Confluent"
FOCUS_VERSION = "1.1"
API = "https://api.confluent.cloud/billing/v1/costs"

# product -> (ServiceName, ServiceCategory FOCUS 1.1)
PRODUCTS = {
    "KAFKA":            ("Confluent Cloud Kafka",            "Analytics"),
    "CONNECT":          ("Confluent Cloud Connect",          "Integration"),
    "KSQL":             ("Confluent Cloud ksqlDB",           "Analytics"),
    "FLINK":            ("Confluent Cloud Flink",            "Analytics"),
    "TABLEFLOW":        ("Confluent Cloud Tableflow",        "Analytics"),
    "CLUSTER_LINK":     ("Confluent Cloud Cluster Linking",  "Networking"),
    "NETWORKING":       ("Confluent Cloud Networking",       "Networking"),
    "GOVERNANCE":       ("Confluent Cloud Stream Governance", "Management and Governance"),
    "SCHEMA_REGISTRY":  ("Confluent Cloud Schema Registry",  "Management and Governance"),
    "SUPPORT":          ("Confluent Cloud Support",          "Other"),
    "CREDIT":           ("Confluent Cloud Credits",          "Other"),
}
RESOURCE_TYPES = {"lkc": "Kafka Cluster", "lcc": "Connector", "lfcp": "Flink Compute Pool",
                  "lsrc": "Schema Registry", "lksqlc": "ksqlDB Cluster", "n": "Network",
                  "env": "Environment", "cl": "Cluster Link"}

COLUMNS = [
    # 21 Mandatory FOCUS 1.1
    "BilledCost", "BillingAccountId", "BillingAccountName", "BillingCurrency",
    "BillingPeriodEnd", "BillingPeriodStart", "ChargeCategory", "ChargeClass",
    "ChargeDescription", "ChargePeriodEnd", "ChargePeriodStart", "ContractedCost",
    "EffectiveCost", "InvoiceIssuerName", "ListCost", "PricingQuantity", "PricingUnit",
    "ProviderName", "PublisherName", "ServiceCategory", "ServiceName",
    # Condicionales / recomendadas
    "ChargeFrequency", "ConsumedQuantity", "ConsumedUnit", "ListUnitPrice",
    "ResourceId", "ResourceName", "ResourceType", "SkuId", "SubAccountId", "SubAccountName", "Tags",
    # Custom
    "x_Product", "x_LineType", "x_NetworkAccessType", "x_DiscountAmount", "x_EnvironmentId",
]
MANDATORY_NOT_NULL = [c for c in COLUMNS[:21] if c not in ("ChargeClass",)]
NULLABLE_FOR_CREDIT = {"PricingQuantity", "PricingUnit"}


# --------------------------------------------------------------------------- API
def _get(url, key, secret):
    req = urllib.request.Request(url, headers={
        "Authorization": "Basic " + base64.b64encode(f"{key}:{secret}".encode()).decode(),
        "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def fetch_live(start: date, end: date):
    key, secret = os.environ.get("CONFLUENT_API_KEY"), os.environ.get("CONFLUENT_API_SECRET")
    if not key or not secret:
        sys.exit("Faltan CONFLUENT_API_KEY / CONFLUENT_API_SECRET en el entorno.")
    items, s = [], start
    while s < end:                                   # ventanas de máximo 1 mes
        e = min(end, (s.replace(day=1) + timedelta(days=32)).replace(day=1))
        url = f"{API}?start_date={s}&end_date={e}"
        while url:
            page = _get(url, key, secret)
            items += page.get("data") or []
            nxt = (page.get("metadata") or {}).get("next")
            url = nxt or None
        s = e
    return items


def load_files(paths):
    items = []
    for p in paths:
        j = json.load(open(p))
        items += j.get("data") or [] if isinstance(j, dict) else j
    return items


# ----------------------------------------------------------------------- mapping
def _d(s):  # "2026-10-29" o "2026-10-29T00:00:00Z" -> date
    return date.fromisoformat(str(s)[:10])


def _iso(d):
    return f"{d:%Y-%m-%d}T00:00:00Z"


def _num(x):
    return float(x) if x not in (None, "") else 0.0


def _fmt(x):
    return f"{x:.6f}"


def to_focus(item, org_id, org_name, env_names, currency):
    line = item.get("line_type") or (item.get("product") or "OTHER")
    is_credit_line = "CREDIT" in line.upper() or "PROMO" in line.upper()
    product = (item.get("product") or ("CREDIT" if is_credit_line else "OTHER")).upper()
    svc, cat = PRODUCTS.get(product, (f"Confluent Cloud {product.title()}", "Other"))
    res = item.get("resource") or {}
    rid = res.get("id") or ""
    env = (res.get("environment") or {}).get("id") or ""
    amount, original = _num(item.get("amount")), _num(item.get("original_amount"))
    discount = _num(item.get("discount_amount"))
    qty, price = _num(item.get("quantity")), item.get("price")
    unit = item.get("unit") or "Units"
    s, e = _d(item["start_date"]), _d(item["end_date"])
    bp_s = s.replace(day=1)
    bp_e = (bp_s + timedelta(days=32)).replace(day=1)
    credit = amount < 0 or is_credit_line
    nat = item.get("network_access_type") or ""
    tags = {k: v for k, v in {
        "confluent/environment": env, "confluent/product": product,
        "confluent/line_type": line, "confluent/network_access_type": nat,
    }.items() if v}
    return {
        "BilledCost": _fmt(amount),
        "BillingAccountId": org_id,
        "BillingAccountName": org_name,
        "BillingCurrency": currency,
        "BillingPeriodEnd": _iso(bp_e),
        "BillingPeriodStart": _iso(bp_s),
        "ChargeCategory": "Credit" if credit else "Usage",
        "ChargeClass": "",
        "ChargeDescription": f"{svc} - {line}" + (f" ({rid})" if rid else ""),
        "ChargePeriodEnd": _iso(e),
        "ChargePeriodStart": _iso(s),
        "ContractedCost": _fmt(amount),
        "EffectiveCost": _fmt(amount),
        "InvoiceIssuerName": "Confluent",
        "ListCost": _fmt(original if original else amount),
        "PricingQuantity": "" if credit and not item.get("quantity") else _fmt(qty),
        "PricingUnit": "" if credit and not item.get("unit") else unit,
        "ProviderName": "Confluent",
        "PublisherName": "Confluent",
        "ServiceCategory": cat,
        "ServiceName": svc,
        "ChargeFrequency": "Usage-Based",
        "ConsumedQuantity": "" if credit else _fmt(qty),
        "ConsumedUnit": "" if credit else unit,
        "ListUnitPrice": "" if price in (None, "") else _fmt(_num(price)),
        "ResourceId": rid,
        "ResourceName": res.get("display_name") or "",
        "ResourceType": RESOURCE_TYPES.get(rid.split("-")[0], "") if rid else "",
        "SkuId": f"{product}:{line}",
        "SubAccountId": env,
        "SubAccountName": env_names.get(env, ""),
        "Tags": json.dumps(tags, separators=(",", ":")),
        "x_Product": product,
        "x_LineType": line,
        "x_NetworkAccessType": nat,
        "x_DiscountAmount": _fmt(discount),
        "x_EnvironmentId": env,
    }


# ----------------------------------------------------------------------- output
def validate(rows):
    errs = []
    for i, r in enumerate(rows):
        for c in MANDATORY_NOT_NULL:
            if r[c] == "" and not (r["ChargeCategory"] == "Credit" and c in NULLABLE_FOR_CREDIT):
                errs.append(f"fila {i}: {c} vacío")
        if r["ChargeCategory"] == "Usage" and float(r["ConsumedQuantity"] or 0) < 0:
            errs.append(f"fila {i}: ConsumedQuantity negativo")
    return errs


def write_drop(out, root_dir, sub_dir, prefix, period_start, rows, epoch):
    bp_e = (period_start + timedelta(days=32)).replace(day=1)
    rel = "/".join(x for x in [sub_dir, VENDOR, f"{period_start:%Y%m%d}-{bp_e:%Y%m%d}", str(epoch)] if x)
    d = os.path.join(out, rel)
    os.makedirs(d, exist_ok=False)
    rows.sort(key=lambda r: (r["ChargePeriodStart"], r["SubAccountId"], r["ResourceId"], r["SkuId"]))
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    w.writeheader(); w.writerows(rows)
    name = f"{prefix}-00001.csv.gz"
    with open(os.path.join(d, name), "wb") as f, gzip.GzipFile(fileobj=f, mode="wb", mtime=0, filename="") as gz:
        gz.write(buf.getvalue().encode())
    key = f"{rel}/{name}"
    manifest = {
        "compression": "GZIP", "content_type": "CSV",
        "report_id": str(uuid.UUID(hashlib.md5(key.encode()).hexdigest())),
        "root_dir": root_dir, "all_report_keys": [key],
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "focus_version": FOCUS_VERSION,
    }
    with open(os.path.join(d, f"{prefix}-Manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return rel


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--live", action="store_true")
    src.add_argument("--input", nargs="+")
    ap.add_argument("--start", help="YYYY-MM-DD (UTC, inclusivo) para --live")
    ap.add_argument("--end", help="YYYY-MM-DD (UTC, exclusivo) para --live")
    ap.add_argument("--root-dir", required=True, help="Bucket S3 / contenedor Azure / bucket GCS")
    ap.add_argument("--sub-dir", default="finops-demo")
    ap.add_argument("--prefix", default="confluent-focus", help="Manifest Prefix en Cloudability")
    ap.add_argument("--org-id", required=True, help="ID de la organización Confluent")
    ap.add_argument("--org-name", default="Confluent Cloud Organization")
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--env-names", nargs="*", default=[], help="env-id=nombre ...")
    ap.add_argument("--out", default="out")
    ap.add_argument("--save-raw", help="Guardar la respuesta cruda de la API en este archivo")
    a = ap.parse_args()

    if a.live:
        if not (a.start and a.end):
            ap.error("--live requiere --start y --end")
        items = fetch_live(_d(a.start), _d(a.end))
        if a.save_raw:
            json.dump({"data": items}, open(a.save_raw, "w"), indent=2)
    else:
        items = load_files(a.input)

    if not items:
        sys.exit("La API no devolvió líneas de costo para ese rango (data vacío). Nada que escribir.")

    env_names = dict(x.split("=", 1) for x in a.env_names)
    by_month = defaultdict(list)
    for it in items:
        r = to_focus(it, a.org_id, a.org_name, env_names, a.currency)
        by_month[_d(it["start_date"]).replace(day=1)].append(r)

    epoch = int(datetime.now(timezone.utc).timestamp())
    for m in sorted(by_month):
        rows = by_month[m]
        errs = validate(rows)
        if errs:
            sys.exit("Validación FOCUS falló:\n  " + "\n  ".join(errs[:20]))
        rel = write_drop(a.out, a.root_dir, a.sub_dir, a.prefix, m, rows, epoch)
        total = sum(float(r["BilledCost"]) for r in rows)
        print(f"{m:%Y-%m}: {len(rows):>4} filas  {total:>10,.2f} {a.currency}  -> {rel}")


if __name__ == "__main__":
    main()
