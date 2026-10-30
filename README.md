# Confluent Cloud → FOCUS 1.1 → IBM Cloudability

Esta solución lleva los costos de **Confluent Cloud** a **IBM Cloudability** usando el estándar
[FOCUS](https://focus.finops.org/), de forma automática y diaria.

```
                    ┌──────────────── Tu cuenta AWS ────────────────┐
Confluent Cloud     │  EventBridge (diario)                          │      IBM Cloudability
Billing API  ◄──────┼── Lambda confluent-focus-export ──► S3 bucket ─┼────►  Custom Data
/billing/v1/costs   │      │  (convierte a FOCUS 1.1)     (privado)   │      (FOCUS Ingress)
                    │      └─ Secrets Manager (API key)               │
                    └────────────────────────────────────────────────┘
```

- **Lambda** (Python 3.12, sin dependencias externas). Cada día lee el mes en curso de la API de
  costos y lo convierte a FOCUS 1.1. Escribe el CSV y el manifest con la estructura que exige
  Cloudability.
- **Secrets Manager** guarda la API key de Confluent. No queda en el código ni en el template.
- **Cloudability** lee el bucket con un rol IAM de solo lectura y descarga los datos una vez al día.
- **Costo en AWS:** casi nulo (unos US$ 0.40 al mes por el secret, más unos centavos de Lambda y S3).

---

## Contenido del repositorio

| Ruta | Qué es |
|---|---|
| `lambda/handler.py` | Lambda: lee la API, genera los drops FOCUS y los sube a S3 |
| `lambda/confluent_to_focus.py` | Conversor Confluent → FOCUS 1.1. También se puede usar como CLI local |
| `template.yaml` | CloudFormation: Lambda, rol IAM con permisos mínimos, schedule, logs y alarma opcional |
| `deploy.sh` | Despliegue en un solo comando: bucket, secret, código, stack y primera ejecución |
| `examples/sample_costs_response.json` | Respuesta real de la API, anonimizada |
| `tests/` | Pruebas de punta a punta con AWS simulado (moto) |

---

## Requisitos

| Dónde | Qué necesitas |
|---|---|
| Confluent Cloud | Una **Cloud API key** (no una key de clúster) con rol **OrganizationAdmin** o **BillingAdmin**, y el **ID de la organización** |
| AWS | Una cuenta con permisos para crear S3, Secrets Manager, IAM, Lambda, EventBridge y CloudFormation |
| Cloudability | Rol de administrador para crear credenciales en *Vendor Credentials* |

---

## Paso 1: Crear la Cloud API key en Confluent

1. En la consola de Confluent Cloud, ve a **Administration → API keys → + Add key**.
2. Elige el alcance **Cloud resource management**, no *Kafka cluster*.
3. Asígnala a una cuenta (usuario o *service account*) con rol **BillingAdmin** u **OrganizationAdmin**.
4. Guarda la **Key** y el **Secret**. El secret solo se muestra una vez.
5. Anota el **Organization ID**: está en *Administration → Organization settings*. También aparece
   en el campo `resource_name` de `GET /org/v2/environments`, como `crn://confluent.cloud/organization=<ID>/...`.

**Prueba opcional** con curl, Bruno o Postman. La autenticación es Basic: usuario = Key, contraseña = Secret.

```bash
curl -s -u "$KEY:$SECRET" \
  "https://api.confluent.cloud/billing/v1/costs?start_date=2026-10-01&end_date=2026-10-30"
```

> **Reglas de la API:** las fechas van en UTC, `end_date` es **exclusivo**, el rango máximo es de
> 1 mes y se puede consultar hasta 1 año atrás. Los costos aparecen con **hasta 72 h de retraso**.

---

## Paso 2: Desplegar en AWS

La forma más simple es **AWS CloudShell**, que ya viene autenticado y no requiere instalar nada.

1. En la consola de AWS, elige la región (por ejemplo `us-east-1`) y abre **CloudShell** (ícono `>_`).
2. Sube el repositorio con **Actions → Upload file** (como zip) o clónalo:
   ```bash
   git clone https://github.com/<tu-usuario>/confluent-focus-cloudability.git
   cd confluent-focus-cloudability
   ```
3. Ejecuta el despliegue:
   ```bash
   BUCKET=<nombre-unico-del-bucket> \
   ORG_ID=<CONFLUENT_ORG_ID> \
   ORG_NAME="Mi Empresa - Confluent" \
   REGION=us-east-1 \
   ALERT_EMAIL=<tu-email-opcional> \
   ./deploy.sh
   ```
4. Cuando lo pida, pega la **API Key** y luego el **Secret**. El secret no se ve al escribirlo.

El script hace todo esto:
- Crea el bucket privado y cifrado si no existe.
- Guarda la key en Secrets Manager (`confluent/billing-api`).
- Sube el código y despliega el stack `confluent-focus-export`.
- **Ejecuta la Lambda una vez** e imprime los valores que necesitas para Cloudability.

Así se ve una salida correcta:
```json
{"today": "2026-10-30", "months": [{"month": "2026-10", "rows": 8, "billed": 0.0, "list_usage": 7.8497, ...}], "uploaded": [...]}
```

Variables opcionales:

| Variable | Default | Uso |
|---|---|---|
| `SUB_DIR` | `finops-demo` | Subcarpeta dentro del bucket (= *Sub Directories* en Cloudability) |
| `PREFIX` | `confluent-focus` | *Manifest Prefix* en Cloudability |
| `STACK` | `confluent-focus-export` | Nombre del stack |
| `ROTATE_SECRET=yes` | — | Vuelve a pedir la key para reemplazarla |

> Si prefieres trabajar desde tu máquina, instala el [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
> y autentícate con `aws configure sso` (IAM Identity Center) o con `aws configure` (access keys).
> Luego ejecuta el mismo comando.

---

## Paso 3: Conectar Cloudability

1. Ve a **Settings → Vendor Credentials → Add Datasource → Other Data Sources → Add a Credential**.
2. Completa los campos con los valores que imprimió `deploy.sh`:

   | Campo | Valor |
   |---|---|
   | Storage | AWS Simple Storage Service |
   | Vendor | `Confluent` |
   | AWS Account ID | cuenta AWS donde está el bucket |
   | Root Directory | nombre del bucket |
   | Sub Directories | `finops-demo` (o el valor de `SUB_DIR`) |
   | Manifest Prefix | `confluent-focus` |

   El *Sample path* del formulario debe quedar así:
   `<bucket>/finops-demo/Confluent/<YYYYMMDD-YYYYMMDD>/<epoch>/confluent-focus-Manifest.json`

3. Pulsa **Generate Template**. En la consola de AWS, ve a **CloudFormation → Create stack → Upload
   a template file**, marca *"I acknowledge that AWS CloudFormation might create IAM resources"* y crea
   el stack. Esto crea el rol `CloudabilityCustomDataRole`.
4. Vuelve a Cloudability y pulsa **Verify Credential**. Debe aparecer el check verde y el permiso
   **(AWS) Custom Data Report** habilitado.

> El check verde solo valida el **acceso** al bucket. Cloudability descarga los datos **una vez al día**,
> así que pueden tardar **hasta 24 h** en aparecer.

---

## Paso 4: Verificar

- Entra a **Cost Reports**, filtra por **Vendor = Confluent** y agrupa por:
  - **Service Name**: Kafka, Connect, Flink, etc.
  - **Resource**: clúster o conector.
  - **Sub Account**: environment.
  - **Charge Category**: separa el uso de los créditos.
- Compara el total con la consola de Confluent (*Billing & payment → Cost*).

---

## Cómo funciona

### Ciclo diario
- La Lambda corre todos los días a las **11:00 UTC** (06:00 en Lima). Se cambia con el parámetro
  `ScheduleExpression` del template.
- En cada ejecución regenera el **mes en curso completo hasta la fecha**, que es lo que exige
  Cloudability, en una **carpeta epoch nueva**. Cloudability toma siempre la más reciente.
- Durante los **primeros 4 días de cada mes** también regenera el mes anterior, para recoger los
  costos que llegan con 72 h de retraso.
- Maneja la paginación de la API (`metadata.next`) y parte los rangos en ventanas de 1 mes.

### Estructura en S3
```
<bucket>/
  _lambda/                                   ← código de la Lambda (Cloudability no lo lee)
  finops-demo/Confluent/20261001-20261101/
    1793333548/
      confluent-focus-00001.csv.gz           ← datos FOCUS 1.1 (CSV gzip)
      confluent-focus-Manifest.json          ← manifest ("M" mayúscula)
```

### Mapeo Confluent → FOCUS 1.1

| Confluent (`/billing/v1/costs`) | FOCUS |
|---|---|
| `amount` | BilledCost, EffectiveCost, ContractedCost |
| `original_amount` | ListCost |
| `price` / `quantity` / `unit` | ListUnitPrice / PricingQuantity / PricingUnit (y ConsumedQuantity / ConsumedUnit en uso) |
| `start_date` / `end_date` | ChargePeriodStart / ChargePeriodEnd |
| mes calendario de `start_date` | BillingPeriodStart / BillingPeriodEnd |
| `product` | ServiceName (`Confluent Cloud Kafka`, `... Connect`, etc.) y ServiceCategory |
| `product:line_type` | SkuId |
| `resource.id` / `resource.display_name` | ResourceId / ResourceName (ResourceType según el prefijo `lkc-`, `lcc-`, `lfcp-`…) |
| `resource.environment.id` | SubAccountId (el nombre del environment se obtiene de `/org/v2/environments`) |
| Organization ID | BillingAccountId |
| `line_type` con `CREDIT`/`PROMO`, o `amount < 0` | ChargeCategory = **Credit** (ServiceName `Confluent Cloud Credits`) |
| — | ProviderName = PublisherName = InvoiceIssuerName = `Confluent` |
| `discount_amount`, `line_type`, `network_access_type` | columnas `x_*` y Tags `confluent/*` |

| `product` | ServiceCategory |
|---|---|
| KAFKA, KSQL, FLINK, TABLEFLOW | Analytics |
| CONNECT | Integration |
| CLUSTER_LINK, NETWORKING | Networking |
| GOVERNANCE, SCHEMA_REGISTRY | Management and Governance |
| SUPPORT, créditos, otros | Other |

---

## Operación

```bash
# Ejecutar ahora (fuera del horario)
aws lambda invoke --function-name confluent-focus-export \
  --cli-binary-format raw-in-base64-out --payload '{}' out.json && cat out.json

# Regenerar meses específicos
aws lambda invoke --function-name confluent-focus-export \
  --cli-binary-format raw-in-base64-out --payload '{"months":["2026-09","2026-10"]}' out.json

# Ver logs
aws logs tail /aws/lambda/confluent-focus-export --since 1d

# Cambiar la API key
ROTATE_SECRET=yes BUCKET=<bucket> ORG_ID=<org> ./deploy.sh
```

### Uso local (sin AWS)
```bash
# Desde una respuesta guardada (curl/Bruno/Postman)
python3 lambda/confluent_to_focus.py --input examples/sample_costs_response.json \
  --root-dir <bucket> --org-id <CONFLUENT_ORG_ID> --env-names env-abc123=produccion

# Llamando a la API directamente
export CONFLUENT_API_KEY=... CONFLUENT_API_SECRET=...
python3 lambda/confluent_to_focus.py --live --start 2026-10-01 --end 2026-11-01 \
  --root-dir <bucket> --org-id <CONFLUENT_ORG_ID>
# luego: aws s3 sync out/ s3://<bucket>/
```

### Pruebas
```bash
pip install -r requirements-dev.txt
python -m pytest -q          # pipeline completo con S3/Secrets Manager simulados
cfn-lint template.yaml
```

---

## Troubleshooting

| Síntoma | Causa | Solución |
|---|---|---|
| La API responde `200` con `"data": []` | No hubo consumo en ese rango o los costos aún no llegan (hasta 72 h). Recuerda que `end_date` es exclusivo | Amplía el rango e incluye días de hace 3 o más. Revisa que haya clústeres con `GET /cmk/v2/clusters?environment=<env-id>` |
| `401 invalid API key ... not a Cluster API Key` | Se usó una key de clúster, o la key y el secret se pegaron al revés o con espacios | Crea una **Cloud API key** y ejecuta `ROTATE_SECRET=yes ./deploy.sh` |
| `403` de la API de Confluent | La cuenta de la key no tiene el rol de billing | Asigna **BillingAdmin** u **OrganizationAdmin** |
| `deploy.sh` falla al crear el bucket | El nombre ya existe en AWS (los nombres son globales) | Usa otro nombre en `BUCKET` |
| `AccessDenied` al desplegar el stack | Tu usuario AWS no puede crear roles IAM o Lambdas | Pide permisos o usa un rol de administrador |
| Check verde en Cloudability pero sin datos después de 24 h | El *Sub Directories* o el *Manifest Prefix* no coinciden con S3 | Compara el *Sample path* del formulario con la ruta real en S3. Si coinciden, abre un caso con soporte de Apptio |
| El gasto de Confluent aparece duplicado | Confluent se paga por el **marketplace de AWS, Azure o GCP**, y ese cargo ya llega en la factura del proveedor de nube | Excluye la línea de marketplace con un Business Mapping, o usa solo una de las dos fuentes |
| BilledCost neto = 0 | La organización tiene créditos promocionales o de compromiso (líneas `Credit`) | Es correcto. Para ver el consumo, filtra `ChargeCategory = Usage` o usa ListCost |

---

## Opcional: FOCUS Agent de Cloudability

La Lambda también escribe un CSV crudo, sin convertir a FOCUS, en `focus-agent/confluent_costs_mtd.csv`,
para usarlo con el **FOCUS Agent** (*Ingress → FOCUS Agent → Cloud Storage*), que mapea a FOCUS con IA.
Para crear el rol de lectura que pide el Agent (*IAM role delegation*), despliega con:

```bash
CLDY_ACCOUNT_ID=<cuenta AWS de Cloudability> CLDY_EXTERNAL_ID=<external id> \
BUCKET=<bucket> ORG_ID=<org> ./deploy.sh
```

> Usa **una sola** de las dos rutas (Custom Data o FOCUS Agent) para no contar el gasto dos veces.
> Se recomienda Custom Data, porque el mapeo ya es FOCUS y no requiere revisión manual.
> Con el FOCUS Agent, la conexión REST directa a Confluent no sirve para cargas recurrentes: el rango
> de fechas queda fijo en la URL y la API usa paginación por `page_token`.

---

## Desinstalar

```bash
aws cloudformation delete-stack --stack-name confluent-focus-export
aws secretsmanager delete-secret --secret-id confluent/billing-api --recovery-window-in-days 7
aws s3 rb s3://<bucket> --force        # borra el bucket y su contenido
```
En Cloudability, archiva la credencial en *Vendor Credentials* y borra el stack del rol
`CloudabilityCustomDataRole`.

---

## Limitaciones conocidas

- El dataset sigue la especificación FOCUS 1.1 y la estructura de *Connect Custom Data* de IBM
  (Cloudability acepta FOCUS 1.0 y 1.1), pero no se validó con el `focus-validator` oficial.
- Organizaciones de Confluent creadas antes del 15 de mayo de 2024 pueden recibir el
  [formato legacy](https://docs.confluent.io/cloud/current/billing/legacy-billing.html) de la API,
  que no está probado.
- Se asume moneda **USD**. Ajusta `--currency` o la Lambda si tu contrato es en otra moneda.

## Referencias

- [Confluent: Retrieve Invoices and Costs](https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html)
- [IBM Cloudability: Connect Custom Data (FOCUS Ingress)](https://www.ibm.com/docs/en/cloudability-commercial/cloudability-enterprise/saas?topic=cloudability-connect-custom-data-focus-ingress)
- [FOCUS Specification](https://focus.finops.org/)
