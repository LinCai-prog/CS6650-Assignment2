#!/usr/bin/env bash
set -euo pipefail
# Run in AWS CloudShell or an existing authenticated AWS CLI session.
REGION="${AWS_REGION:-us-west-2}"
ALB_NAME="${ALB_NAME:-cs6650-ingest-alb}"
TG_NAME="${TG_NAME:-cs6650-ingest-tg}"
: "${EXPECTED_TARGET_IDS:?Set EXPECTED_TARGET_IDS to the four space-separated ingest instance IDs}"
BACKUP_DIR="alb-config-before-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_DIR"
alb_arn=$(aws elbv2 describe-load-balancers --region "$REGION" --names "$ALB_NAME" --query 'LoadBalancers[0].LoadBalancerArn' --output text)
tg_arn=$(aws elbv2 describe-target-groups --region "$REGION" --names "$TG_NAME" --query 'TargetGroups[0].TargetGroupArn' --output text)
aws elbv2 describe-load-balancer-attributes --region "$REGION" --load-balancer-arn "$alb_arn" > "$BACKUP_DIR/alb-attributes.json"
aws elbv2 describe-target-group-attributes --region "$REGION" --target-group-arn "$tg_arn" > "$BACKUP_DIR/target-attributes.json"
aws elbv2 describe-target-groups --region "$REGION" --target-group-arns "$tg_arn" > "$BACKUP_DIR/target-group.json"
aws elbv2 describe-target-health --region "$REGION" --target-group-arn "$tg_arn" > "$BACKUP_DIR/target-health.json"
python3 - "$BACKUP_DIR/target-health.json" "$EXPECTED_TARGET_IDS" <<'PY'
import json,sys
expected=set(sys.argv[2].split())
if len(expected) != 4 or not all(v.startswith('i-') for v in expected):
    raise SystemExit('Expected four unique EC2 instance IDs; no configuration changed.')
actual={t['Target']['Id'] for t in json.load(open(sys.argv[1]))['TargetHealthDescriptions']}
if actual != expected:raise SystemExit('Target membership differs from the four inspected ingest hosts; no configuration changed.')
PY
aws elbv2 modify-target-group-attributes --region "$REGION" --target-group-arn "$tg_arn" --attributes Key=stickiness.enabled,Value=true Key=stickiness.type,Value=lb_cookie Key=stickiness.lb_cookie.duration_seconds,Value=300
aws elbv2 modify-load-balancer-attributes --region "$REGION" --load-balancer-arn "$alb_arn" --attributes Key=idle_timeout.timeout_seconds,Value=120
aws elbv2 modify-target-group --region "$REGION" --target-group-arn "$tg_arn" --health-check-path /server-v2/health --health-check-interval-seconds 30 --health-check-timeout-seconds 5 --healthy-threshold-count 2 --unhealthy-threshold-count 3
aws elbv2 describe-target-group-attributes --region "$REGION" --target-group-arn "$tg_arn"
aws elbv2 describe-load-balancer-attributes --region "$REGION" --load-balancer-arn "$alb_arn"
printf '\nPrevious configuration saved in %s\n' "$BACKUP_DIR"
