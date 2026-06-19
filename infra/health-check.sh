#!/usr/bin/env bash
# =============================================================================
# health-check.sh — Vérifie que les services sont prêts avant un lab
# Usage: ./infra/health-check.sh [profil]
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'
BOLD='\033[1m'; NC='\033[0m'

PROFILE="${1:-infra}"
ALL_OK=true

check() {
  local name="$1" url="$2"
  if curl -sf "$url" > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} ${name}"
  else
    echo -e "  ${RED}✗${NC} ${name} — ${YELLOW}${url}${NC}"
    ALL_OK=false
  fi
}

echo ""
echo -e "${BOLD}Health check — profil: ${PROFILE}${NC}"
echo ""

echo "Services de base (profil: infra):"
check "MinIO"    "http://localhost:9000/minio/health/live"
check "Nessie"   "http://localhost:19120/api/v2/config"
check "Polaris"  "http://localhost:8181/api/catalog/v1/config"
check "Trino"    "http://localhost:8080/v1/info"
check "Jupyter"  "http://localhost:8888/api"

case "$PROFILE" in
  iceberg)
    echo ""
    echo "Services Iceberg labs:"
    check "Dremio UI"    "http://localhost:9047/apiv2/server_status"
    check "Spark Master"  "http://localhost:8085"
    ;;
  cicd)
    echo ""
    echo "Services CI/CD Data:"
    check "Airflow"  "http://localhost:8089/health"
    ;;
  mlops)
    echo ""
    echo "Services MLOps:"
    check "MLflow"       "http://localhost:5000/api/2.0/mlflow/experiments/list"
    check "Feast Server" "http://localhost:6566"
    check "Redis"        "$(redis-cli -h localhost ping 2>/dev/null || echo NOK)"
    ;;
  governance)
    echo ""
    echo "Services Gouvernance:"
    check "OpenMetadata" "http://localhost:8585/api/v1/system/status"
    ;;
  cdc)
    echo ""
    echo "Services CDC (démo formateur):"
    check "Kafka UI"  "http://localhost:8087"
    check "Debezium"  "http://localhost:8083/connectors"
    check "Postgres"  "$(pg_isready -h localhost -U retailco 2>/dev/null && echo OK || echo NOK)"
    ;;
esac

echo ""
if [ "$ALL_OK" = true ]; then
  echo -e "${GREEN}${BOLD}✅ Tous les services sont prêts — bon lab !${NC}"
else
  echo -e "${RED}${BOLD}❌ Certains services ne répondent pas.${NC}"
  echo -e "   → docker compose ps"
  echo -e "   → docker compose logs <service>"
fi
echo ""
