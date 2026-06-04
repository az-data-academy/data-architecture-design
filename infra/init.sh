#!/usr/bin/env bash
# =============================================================================
# init.sh — Initialisation complète de la stack formation
# Idempotent : peut être relancé sans erreur
# =============================================================================
set -euo pipefail

BOLD='\033[1m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log()     { echo -e "${BLUE}[init]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
warn()    { echo -e "${YELLOW}[!]${NC} $1"; }
error()   { echo -e "${RED}[✗]${NC} $1"; exit 1; }

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  Formation Data Lakehouse — Initialisation stack     ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ── 1. Attendre MinIO ────────────────────────────────────────────────────────
log "Attente de MinIO..."
for i in $(seq 1 30); do
  if curl -sf http://minio:9000/minio/health/live > /dev/null 2>&1; then
    success "MinIO opérationnel"
    break
  fi
  [ $i -eq 30 ] && error "MinIO non disponible après 60s"
  sleep 2
done

# ── 2. Buckets MinIO ─────────────────────────────────────────────────────────
log "Création des buckets MinIO..."
pip install awscli-local > /dev/null 2>&1 || true

aws_local() {
  aws --endpoint-url http://minio:9000 \
      --region us-east-1 \
      --no-sign-request \
      --output text \
      "$@" 2>/dev/null || true
}

for bucket in bronze silver gold warehouse mlflow feast; do
  aws_local s3 mb "s3://${bucket}" 2>/dev/null || true
  success "Bucket s3://${bucket}"
done

# ── 3. Attendre Nessie ───────────────────────────────────────────────────────
log "Attente de Nessie..."
for i in $(seq 1 30); do
  if curl -sf http://nessie:19120/api/v2/config > /dev/null 2>&1; then
    success "Nessie opérationnel"
    break
  fi
  [ $i -eq 30 ] && { warn "Nessie non disponible — ignoré"; break; }
  sleep 2
done

# ── 4. Branches Nessie ───────────────────────────────────────────────────────
log "Configuration des branches Nessie..."
NESSIE_API="http://nessie:19120/api/v2"

# Vérifier si la branche feat/nessie-demo existe
MAIN_HASH=$(curl -sf "${NESSIE_API}/trees/main" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['reference']['hash'])" 2>/dev/null || echo "")

if [ -n "$MAIN_HASH" ]; then
  # Créer la branche feat/nessie-demo si elle n'existe pas
  curl -sf -X POST "${NESSIE_API}/trees" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"feat/nessie-demo\",\"type\":\"BRANCH\",\"hash\":\"${MAIN_HASH}\"}" \
    > /dev/null 2>&1 || true
  success "Branche Nessie feat/nessie-demo créée"

  # Branche dédiée CI/CD
  curl -sf -X POST "${NESSIE_API}/trees" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"feat/silver-retailco\",\"type\":\"BRANCH\",\"hash\":\"${MAIN_HASH}\"}" \
    > /dev/null 2>&1 || true
  success "Branche Nessie feat/silver-retailco créée"
else
  warn "Nessie non joignable — branches non créées"
fi

# ── 5. Attendre Polaris ──────────────────────────────────────────────────────
log "Attente de Polaris..."
for i in $(seq 1 30); do
  if curl -sf http://polaris:8181/api/catalog/v1/config > /dev/null 2>&1; then
    success "Polaris opérationnel"
    break
  fi
  [ $i -eq 30 ] && { warn "Polaris non disponible — ignoré"; break; }
  sleep 2
done

# ── 6. Namespaces Polaris ────────────────────────────────────────────────────
log "Création des namespaces Polaris..."
POLARIS_API="http://polaris:8181/api/catalog"

# Catalogue principal
curl -sf -X POST "${POLARIS_API}/v1/namespaces" \
  -H "Content-Type: application/json" \
  -d '{"namespace":["retailco"],"properties":{"owner":"formation"}}' \
  > /dev/null 2>&1 || true
success "Namespace Polaris: retailco"

curl -sf -X POST "${POLARIS_API}/v1/namespaces" \
  -H "Content-Type: application/json" \
  -d '{"namespace":["mlops"],"properties":{"owner":"formation"}}' \
  > /dev/null 2>&1 || true
success "Namespace Polaris: mlops"

# ── 7. Attendre Trino ────────────────────────────────────────────────────────
log "Attente de Trino..."
for i in $(seq 1 40); do
  if curl -sf http://trino:8080/v1/info > /dev/null 2>&1; then
    success "Trino opérationnel"
    break
  fi
  [ $i -eq 40 ] && { warn "Trino non disponible — ignoré"; break; }
  sleep 3
done

# ── 8. Attendre JupyterLab ───────────────────────────────────────────────────
log "Attente de JupyterLab..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8888/api > /dev/null 2>&1; then
    success "JupyterLab opérationnel"
    break
  fi
  sleep 2
done

# ── 9. Copie des données RetailCo ────────────────────────────────────────────
log "Chargement du dataset RetailCo..."
if [ -f "/home/jovyan/data/retailco_transactions_sample.csv" ]; then
  aws_local s3 cp \
    /home/jovyan/data/retailco_transactions_sample.csv \
    s3://bronze/retailco/transactions/retailco_transactions_sample.csv \
    || true
  success "Dataset RetailCo chargé dans s3://bronze/retailco/"
fi

# ── Résumé ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  Stack initialisée — accès aux interfaces            ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  JupyterLab    → ${GREEN}http://localhost:8888${NC}  (token: formation2024)"
echo -e "  MinIO Console → ${GREEN}http://localhost:9001${NC}  (admin/minioadmin)"
echo -e "  Trino UI      → ${GREEN}http://localhost:8080${NC}"
echo -e "  Polaris API   → ${GREEN}http://localhost:8181${NC}"
echo -e "  Nessie API    → ${GREEN}http://localhost:19120${NC}"
echo ""
echo -e "  ${BOLD}Démarrer un atelier :${NC}"
echo -e "  ./infra/switch_profile.sh iceberg    # J1 — Iceberg labs"
echo -e "  ./infra/switch_profile.sh cicd       # J2-A — Nessie CI/CD"
echo -e "  ./infra/switch_profile.sh mlops      # J2-B — Feature Store"
echo -e "  ./infra/switch_profile.sh governance # J3 — Gouvernance"
echo ""
