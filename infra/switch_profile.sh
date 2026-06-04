#!/usr/bin/env bash
# =============================================================================
# switch_profile.sh — Change de profil d'atelier sans perdre les données
# Les volumes MinIO (tables Iceberg) et Nessie (branches) sont préservés
#
# Usage:
#   ./infra/switch_profile.sh <nouveau_profil>
#   ./infra/switch_profile.sh iceberg
#   ./infra/switch_profile.sh mlops
#   Profils : iceberg | cicd | mlops | governance | cdc
# =============================================================================
set -euo pipefail

BOLD='\033[1m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; NC='\033[0m'

VALID_PROFILES=(iceberg cicd mlops governance cdc)
NEW_PROFILE="${1:-}"

# Validation
if [ -z "$NEW_PROFILE" ]; then
  echo -e "${YELLOW}Usage: $0 <profil>${NC}"
  echo -e "Profils disponibles: ${VALID_PROFILES[*]}"
  exit 1
fi

valid=false
for p in "${VALID_PROFILES[@]}"; do
  [[ "$p" == "$NEW_PROFILE" ]] && valid=true && break
done
if [ "$valid" == "false" ]; then
  echo -e "${YELLOW}[!] Profil invalide: $NEW_PROFILE${NC}"
  echo -e "Profils disponibles: ${VALID_PROFILES[*]}"
  exit 1
fi

echo ""
echo -e "${BOLD}[switch_profile] Passage au profil: ${GREEN}${NEW_PROFILE}${NC}"
echo ""

# ── 1. Arrêter les profils non-infra actifs (sans toucher aux volumes) ───────
echo -e "${BLUE}[1/3]${NC} Arrêt des containers du profil précédent..."
for profile in "${VALID_PROFILES[@]}"; do
  if [ "$profile" != "$NEW_PROFILE" ]; then
    docker compose --profile "$profile" stop 2>/dev/null || true
    docker compose --profile "$profile" rm -f 2>/dev/null || true
  fi
done

# ── 2. S'assurer que le profil infra tourne ───────────────────────────────────
echo -e "${BLUE}[2/3]${NC} Vérification du profil infra (MinIO, Polaris, Nessie, Trino, Jupyter)..."
docker compose --profile infra up -d 2>/dev/null

# ── 3. Démarrer le nouveau profil ────────────────────────────────────────────
echo -e "${BLUE}[3/3]${NC} Démarrage du profil ${GREEN}${NEW_PROFILE}${NC}..."
docker compose --profile infra --profile "$NEW_PROFILE" up -d

echo ""
echo -e "${GREEN}[✓] Profil ${BOLD}${NEW_PROFILE}${NC}${GREEN} actif${NC}"
echo ""

# Afficher les URLs utiles selon le profil
case "$NEW_PROFILE" in
  iceberg)
    echo -e "  Spark UI     → ${GREEN}http://localhost:8085${NC}"
    echo -e "  JupyterLab   → ${GREEN}http://localhost:8888${NC}"
    echo -e "  Labs          → labs/j1-iceberg-masterclass/"
    ;;
  cicd)
    echo -e "  Airflow UI   → ${GREEN}http://localhost:8089${NC}  (admin/admin)"
    echo -e "  JupyterLab   → ${GREEN}http://localhost:8888${NC}"
    echo -e "  Labs          → labs/j2-nessie-cicd/"
    ;;
  mlops)
    echo -e "  MLflow UI    → ${GREEN}http://localhost:5000${NC}"
    echo -e "  Feast Server → ${GREEN}http://localhost:6566${NC}"
    echo -e "  JupyterLab   → ${GREEN}http://localhost:8888${NC}"
    echo -e "  Labs          → labs/j2-feature-store/"
    ;;
  governance)
    echo -e "  OpenMetadata → ${GREEN}http://localhost:8585${NC}"
    echo -e "  JupyterLab   → ${GREEN}http://localhost:8888${NC}"
    echo -e "  Labs          → labs/j3-governance/"
    ;;
  cdc)
    echo -e "  Kafka UI     → ${GREEN}http://localhost:8087${NC}"
    echo -e "  Debezium API → ${GREEN}http://localhost:8083${NC}"
    echo -e "  Postgres     → ${GREEN}localhost:5432${NC}  (retailco/retailco)"
    echo -e "  Démo formateur uniquement"
    ;;
esac
echo ""
