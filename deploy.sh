#!/usr/bin/env bash
# Despliega el export Confluent -> FOCUS -> S3 en tu cuenta AWS.
# Requisitos: AWS CLI v2 configurado (aws sts get-caller-identity funciona), zip.
#
# Uso:
#   BUCKET=mi-bucket-focus-confluent ORG_ID=<CONFLUENT_ORG_ID> REGION=us-east-1 \
#   ALERT_EMAIL=yo@empresa.com ./deploy.sh
set -euo pipefail

: "${BUCKET:?Define BUCKET (bucket dedicado para FOCUS)}"
: "${ORG_ID:?Define ORG_ID (organización Confluent)}"
REGION="${REGION:-us-east-1}"
ORG_NAME="${ORG_NAME:-Confluent Cloud Organization}"
SUB_DIR="${SUB_DIR:-finops-demo}"
PREFIX="${PREFIX:-confluent-focus}"
ALERT_EMAIL="${ALERT_EMAIL:-}"
STACK="${STACK:-confluent-focus-export}"
CLDY_ACCOUNT_ID="${CLDY_ACCOUNT_ID:-}"     # FOCUS Agent: cuenta AWS de Cloudability (del formulario)
CLDY_EXTERNAL_ID="${CLDY_EXTERNAL_ID:-}"   # FOCUS Agent: External ID (del formulario)
SECRET_NAME="${SECRET_NAME:-confluent/billing-api}"
cd "$(dirname "$0")"

echo "==> Cuenta: $(aws sts get-caller-identity --query Account --output text)  Región: $REGION"

# 1) Bucket dedicado, privado y cifrado
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "==> Creando bucket $BUCKET"
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
  aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
fi

# 2) Secret con la Cloud API key de Confluent (se pide por teclado, no queda en el historial)
SECRET_ARN=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$REGION" \
  --query ARN --output text 2>/dev/null || true)
if [ -z "$SECRET_ARN" ] || [ "${ROTATE_SECRET:-no}" = "yes" ]; then
  read -r  -p "Confluent API Key: " CC_KEY
  read -rs -p "Confluent API Secret: " CC_SECRET; echo
  PAYLOAD=$(CC_KEY="$CC_KEY" CC_SECRET="$CC_SECRET" python3 -c \
    'import json,os;print(json.dumps({"api_key":os.environ["CC_KEY"].strip(),"api_secret":os.environ["CC_SECRET"].strip()}))')
  if [ -z "$SECRET_ARN" ]; then
    SECRET_ARN=$(aws secretsmanager create-secret --name "$SECRET_NAME" --region "$REGION" \
      --secret-string "$PAYLOAD" --query ARN --output text)
  else
    aws secretsmanager put-secret-value --secret-id "$SECRET_NAME" --region "$REGION" --secret-string "$PAYLOAD" >/dev/null
  fi
  unset CC_KEY CC_SECRET PAYLOAD
fi
echo "==> Secret: $SECRET_ARN"

# 3) Empaquetar y subir el código (fuera de la carpeta que lee Cloudability)
rm -f build.zip && (cd lambda && zip -q ../build.zip handler.py confluent_to_focus.py)
HASH=$( (sha256sum build.zip 2>/dev/null || shasum -a 256 build.zip) | cut -c1-12)
CODE_KEY="_lambda/confluent-focus-export-$HASH.zip"
aws s3 cp build.zip "s3://$BUCKET/$CODE_KEY" --region "$REGION" >/dev/null

# 4) Stack
aws cloudformation deploy --region "$REGION" --stack-name "$STACK" \
  --template-file template.yaml --capabilities CAPABILITY_IAM \
  --parameter-overrides BucketName="$BUCKET" CodeKey="$CODE_KEY" SecretArn="$SECRET_ARN" \
    OrgId="$ORG_ID" OrgName="$ORG_NAME" SubDir="$SUB_DIR" ManifestPrefix="$PREFIX" AlertEmail="$ALERT_EMAIL" \
    CloudabilityAwsAccountId="$CLDY_ACCOUNT_ID" CloudabilityExternalId="$CLDY_EXTERNAL_ID"

# 5) Primera ejecución ahora mismo
echo "==> Ejecutando la Lambda una vez..."
OUT=$(mktemp)
FUNC_ERR=$(aws lambda invoke --region "$REGION" --function-name confluent-focus-export \
  --cli-binary-format raw-in-base64-out --payload '{}' "$OUT" --query FunctionError --output text)
cat "$OUT"; echo; echo
if [ "$FUNC_ERR" != "None" ] && [ -n "$FUNC_ERR" ]; then
  echo "ERROR: la Lambda falló (ver mensaje arriba). Revisa la sección Troubleshooting del README."
  echo "Si es un 401 de Confluent, corrige la key con: ROTATE_SECRET=yes ./deploy.sh"
  exit 1
fi
if [ -n "$CLDY_ACCOUNT_ID" ]; then
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?starts_with(OutputKey,'FocusAgent')].[OutputKey,OutputValue]" --output text
  echo
fi
echo "Listo. En Cloudability: Settings > Vendor Credentials > Add Datasource > Other Data Sources"
echo "  Storage        : AWS Simple Storage Service"
echo "  Vendor         : Confluent"
echo "  AWS Account ID : $(aws sts get-caller-identity --query Account --output text)"
echo "  Root Directory : $BUCKET"
echo "  Sub Directories: $SUB_DIR"
echo "  Manifest Prefix: $PREFIX"
echo "  FOCUS version  : 1.1"
