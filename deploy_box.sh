#!/usr/bin/env bash
# Deploy the Box + Temporal pattern (stack askmybank-box). Leaves the S3 + Orkes stack untouched.
#   ./deploy_box.sh            build + push worker image, build + deploy stack
#   ./deploy_box.sh --stack    stack only (reuse the last pushed worker image)
# Secrets are read from .env at deploy time — nothing secret is written to samconfig.toml.
set -euo pipefail
cd "$(dirname "$0")"

envval() { grep -E "^$1=" .env | tail -1 | cut -d= -f2- | sed -E 's/^ +//; s/^"(.*)"$/\1/'; }
for v in MOTHERDUCK_TOKEN TEMPORAL_ADDRESS TEMPORAL_NAMESPACE TEMPORAL_API_KEY DEMO_API_KEY \
         BOX_CLIENT_ID BOX_CLIENT_SECRET BOX_ENTERPRISE_ID BOX_WEBHOOK_PRIMARY_KEY BOX_WEBHOOK_SECONDARY_KEY; do
  declare "$v=$(envval "$v")"
done
REGION=us-east-1
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REPO=askmybank-box-worker
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

for v in MOTHERDUCK_TOKEN TEMPORAL_ADDRESS TEMPORAL_NAMESPACE TEMPORAL_API_KEY DEMO_API_KEY; do
  [ -n "${!v:-}" ] || { echo "Missing $v in .env"; exit 1; }
done

if [ "${1:-}" != "--stack" ]; then
  aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name "$REPO" --region "$REGION" >/dev/null
  TAG="$(date +%Y%m%d%H%M%S)"
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null
  docker build --platform linux/amd64 -f workflows/temporal/box_pipeline/Dockerfile -t "$REGISTRY/$REPO:$TAG" .
  docker push "$REGISTRY/$REPO:$TAG"
  echo "$REGISTRY/$REPO:$TAG" > .aws-sam/box-worker-image
fi
WORKER_IMAGE=$(cat .aws-sam/box-worker-image)

VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
  --query 'Subnets[].SubnetId' --output text | tr '\t' ',')

OVERRIDES=(
  "MotherDuckToken=$MOTHERDUCK_TOKEN"
  "TemporalAddress=$TEMPORAL_ADDRESS"
  "TemporalNamespace=$TEMPORAL_NAMESPACE"
  "TemporalApiKey=$TEMPORAL_API_KEY"
  "DemoApiKey=$DEMO_API_KEY"
  "WorkerImageUri=$WORKER_IMAGE"
  "VpcId=$VPC_ID"
  "WorkerSubnets=$SUBNETS"
)
for pair in BoxClientId:BOX_CLIENT_ID BoxClientSecret:BOX_CLIENT_SECRET BoxEnterpriseId:BOX_ENTERPRISE_ID \
            BoxWebhookPrimaryKey:BOX_WEBHOOK_PRIMARY_KEY BoxWebhookSecondaryKey:BOX_WEBHOOK_SECONDARY_KEY; do
  param=${pair%%:*}; var=${pair##*:}
  [ -n "${!var:-}" ] && OVERRIDES+=("$param=${!var}")
done

sam build -t template-box.yaml --build-dir .aws-sam/build-box
sam deploy --config-file "$PWD/samconfig.toml" --config-env box \
  --template-file .aws-sam/build-box/template.yaml --parameter-overrides "${OVERRIDES[@]}"

aws cloudformation describe-stacks --stack-name askmybank-box --region "$REGION" \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table
