#!/usr/bin/env sh
# =============================================================================
# init-dremio.sh — Configure Dremio au premier démarrage
# Exécuté par dremio-init (service Docker Compose, profil iceberg)
#
# Actions :
#   1. Créer l'utilisateur admin
#   2. Authentifier et récupérer le token
#   3. Ajouter la source Nessie (catalog Iceberg)
#   4. Ajouter la source MinIO (S3-compatible)
# =============================================================================
set -e

DREMIO="http://dremio:9047"
ADMIN_USER="${DREMIO_ADMIN_USER:-admin}"
ADMIN_PASS="${DREMIO_ADMIN_PASSWORD:-Dremio@2026}"

log()  { echo "[dremio-init] $1"; }
ok()   { echo "[dremio-init] ✅ $1"; }
fail() { echo "[dremio-init] ❌ $1"; exit 1; }

# ── 1. Attendre que Dremio soit prêt ────────────────────────────────────────
log "Attente de Dremio (peut prendre 5-10 min selon la machine)..."
for i in $(seq 1 120); do
  STATUS=$(curl -sf "$DREMIO/apiv2/server_status" 2>/dev/null || echo "")
  if echo "$STATUS" | grep -q '"status":"OK"'; then
    ok "Dremio opérationnel"
    break
  fi
  [ $i -eq 120 ] && fail "Dremio non disponible après 10 min"
  sleep 5
done

# ── 2. Créer l'admin (first-user bootstrap) ──────────────────────────────────
log "Création de l'utilisateur admin..."
BOOTSTRAP=$(curl -sf -X PUT "$DREMIO/apiv2/bootstrap/firstuser" \
  -H "Content-Type: application/json" \
  -d "{
    \"userName\": \"$ADMIN_USER\",
    \"firstName\": \"Admin\",
    \"lastName\": \"Formation\",
    \"email\": \"admin@formation.local\",
    \"password\": \"$ADMIN_PASS\"
  }" 2>&1 || echo "exists")

if echo "$BOOTSTRAP" | grep -q "exists\|already"; then
  log "Admin déjà créé — skip"
else
  ok "Admin créé : $ADMIN_USER / $ADMIN_PASS"
fi

# ── 3. Authentification ───────────────────────────────────────────────────────
log "Authentification..."
TOKEN=$(curl -sf -X POST "$DREMIO/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null)

[ -z "$TOKEN" ] && fail "Impossible d'obtenir le token Dremio"
ok "Token obtenu"
AUTH="Authorization: _dremio$TOKEN"

# Vérifier si les sources existent déjà
SOURCES=$(curl -sf "$DREMIO/apiv2/source/" -H "$AUTH" 2>/dev/null || echo '{"data":[]}')

# ── 4. Ajouter la source Nessie ───────────────────────────────────────────────
if echo "$SOURCES" | python3 -c "import sys,json; d=json.load(sys.stdin); print(any(s.get('name')=='nessie' for s in d.get('data',[])));" 2>/dev/null | grep -q "True"; then
  log "Source Nessie déjà présente — skip"
else
  log "Configuration de la source Nessie..."
  NESSIE_RESP=$(curl -sf -X POST "$DREMIO/apiv2/source/" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{
      "name": "nessie",
      "type": "NESSIE",
      "config": {
        "nessieEndpoint": "http://nessie:19120/api/v2",
        "nessieAuthType": "NONE",
        "credentialType": "ACCESS_KEY",
        "awsAccessKey": "minioadmin",
        "awsAccessSecret": "minioadmin",
        "awsRootPath": "/warehouse",
        "secure": false,
        "propertyList": [
          {"name": "dremio.s3.compat",           "value": "true"},
          {"name": "fs.s3a.path.style.access",   "value": "true"},
          {"name": "fs.s3a.endpoint",             "value": "http://minio:9000"},
          {"name": "fs.s3a.access.key",           "value": "minioadmin"},
          {"name": "fs.s3a.secret.key",           "value": "minioadmin"}
        ]
      }
    }' 2>&1)

  if echo "$NESSIE_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null | grep -qE "[a-z0-9-]{10}"; then
    ok "Source Nessie ajoutée (catalog Iceberg — toutes les branches)"
  else
    log "Nessie: $NESSIE_RESP"
  fi
fi

# ── 5. Ajouter la source MinIO (S3) ──────────────────────────────────────────
if echo "$SOURCES" | python3 -c "import sys,json; d=json.load(sys.stdin); print(any(s.get('name')=='minio' for s in d.get('data',[])));" 2>/dev/null | grep -q "True"; then
  log "Source MinIO déjà présente — skip"
else
  log "Configuration de la source MinIO (S3)..."
  MINIO_RESP=$(curl -sf -X POST "$DREMIO/apiv2/source/" \
    -H "Content-Type: application/json" \
    -H "$AUTH" \
    -d '{
      "name": "minio",
      "type": "S3",
      "config": {
        "credentialType": "ACCESS_KEY",
        "accessKey":      "minioadmin",
        "accessSecret":   "minioadmin",
        "secure": false,
        "externalBucketList": [],
        "rootPath": "/",
        "defaultCtasFormat": "ICEBERG",
        "propertyList": [
          {"name": "fs.s3a.endpoint",           "value": "http://minio:9000"},
          {"name": "fs.s3a.path.style.access",  "value": "true"},
          {"name": "dremio.s3.compat",          "value": "true"}
        ],
        "whitelistedBuckets": ["bronze","silver","gold","warehouse","mlflow","feast"]
      }
    }' 2>&1)

  if echo "$MINIO_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id',''))" 2>/dev/null | grep -qE "[a-z0-9-]{10}"; then
    ok "Source MinIO ajoutée (accès direct aux buckets Bronze/Silver/Gold)"
  else
    log "MinIO S3: $MINIO_RESP"
  fi
fi

# ── Résumé ────────────────────────────────────────────────────────────────────
echo ""
echo "  ╔══════════════════════════════════════════════════╗"
echo "  ║  Dremio configuré — accès                       ║"
echo "  ╠══════════════════════════════════════════════════╣"
echo "  ║  UI       → http://localhost:9047                ║"
echo "  ║  Login    → $ADMIN_USER / $ADMIN_PASS            ║"
echo "  ║  Sources  → nessie (branches Iceberg)            ║"
echo "  ║             minio  (buckets S3)                  ║"
echo "  ╚══════════════════════════════════════════════════╝"
