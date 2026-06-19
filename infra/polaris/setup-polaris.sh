#!/bin/sh
# =============================================================================
# setup-polaris.sh — Initialisation du catalog Polaris après bootstrap
# Basé sur le quickstart officiel Apache Polaris (regtests/docker-compose.yml)
#
# IMPORTANT : utiliser s3a:// (pas s3://) pour la compatibilité Spark S3A
# Spark ne supporte que s3a://, pas le schème s3:// (hadoop legacy)
# =============================================================================
set -e

POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
CATALOG_NAME="${CATALOG_NAME:-retailco}"
REALM="${REALM:-POLARIS}"
MINIO_ENDPOINT_INT="${MINIO_ENDPOINT_INT:-http://minio:9000}"
MINIO_ENDPOINT_EXT="${MINIO_ENDPOINT_EXT:-http://localhost:9000}"

echo "=== Polaris Setup ==="
echo "URI    : $POLARIS_URI"
echo "Catalog: $CATALOG_NAME"
echo "Realm  : $REALM"
echo ""

# ── 1. Obtenir un token OAuth2 root ──────────────────────────────────────────
echo "── Obtention du token OAuth2 root..."
TOKEN_RESPONSE=$(curl -sf -X POST \
  "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL")

TOKEN=$(echo "$TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$TOKEN" ]; then
  echo "❌ Echec obtention du token OAuth2"
  echo "Response: $TOKEN_RESPONSE"
  exit 1
fi
echo "✅ Token obtenu"

# ── 2. Vérifier si le catalog existe déjà (idempotent) ────────────────────────
echo ""
echo "── Vérification du catalog '$CATALOG_NAME'..."
CATALOG_CHECK=$(curl -sf -o /dev/null -w "%{http_code}" \
  "$POLARIS_URI/api/management/v1/catalogs/$CATALOG_NAME" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: $REALM" 2>/dev/null || echo "000")

if [ "$CATALOG_CHECK" = "200" ]; then
  echo "✅ Catalog '$CATALOG_NAME' existe déjà — setup ignoré"
  exit 0
fi

# ── 3. Créer le catalog retailco ──────────────────────────────────────────────
# IMPORTANT : s3a:// pour la compatibilité Spark/Hadoop S3A FileSystem
# s3:// = scheme non supporté par Hadoop → UnsupportedFileSystemException
echo ""
echo "── Création du catalog '$CATALOG_NAME' (s3a:// scheme)..."
CREATE_RESPONSE=$(curl -sf -X POST \
  "$POLARIS_URI/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: $REALM" \
  -d "{
    \"catalog\": {
      \"name\": \"${CATALOG_NAME}\",
      \"type\": \"INTERNAL\",
      \"readOnly\": false,
      \"properties\": {
        \"default-base-location\": \"s3a://warehouse\"
      },
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"s3a://warehouse\", \"s3a://bronze\", \"s3a://silver\", \"s3a://gold\"],
        \"endpoint\": \"${MINIO_ENDPOINT_INT}\",
        \"endpointInternal\": \"${MINIO_ENDPOINT_INT}\",
        \"pathStyleAccess\": true,
        \"region\": \"us-east-1\",
        \"stsUnavailable\": true
      }
    }
  }" 2>&1) || {
  echo "❌ Echec création du catalog"
  echo "$CREATE_RESPONSE"
  exit 1
}
echo "✅ Catalog '$CATALOG_NAME' créé (default-base-location: s3a://warehouse)"

# ── 4. Créer le namespace retailco ────────────────────────────────────────────
echo ""
echo "── Création du namespace 'retailco'..."
curl -sf -X POST \
  "$POLARIS_URI/api/catalog/v1/${CATALOG_NAME}/namespaces" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: $REALM" \
  -d '{"namespace": ["retailco"], "properties": {}}' \
  > /dev/null && echo "✅ Namespace 'retailco' créé" || echo "⚠️  Namespace déjà existant"

# ── 5. Créer un catalog role dédié + grant CATALOG_MANAGE_CONTENT ────────────
# IMPORTANT : le principal root (rôles service_admin/catalog_admin) ne couvre PAS
# les opérations de délégation de credentials (ex: CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION).
# Pattern officiel Polaris quickstart : créer un catalog role dédié et lui accorder
# explicitement CATALOG_MANAGE_CONTENT, puis le lier au principal-role service_admin
# (déjà assigné à root par défaut au bootstrap).
# Source : polaris.apache.org/releases/1.1.0/getting-started/using-polaris/
echo ""
echo "── Création du catalog role 'retailco_admin_role'..."
curl -sf -X POST \
  "$POLARIS_URI/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: $REALM" \
  -d '{"catalogRole": {"name": "retailco_admin_role"}}' \
  > /dev/null && echo "✅ Catalog role 'retailco_admin_role' créé" || echo "⚠️  Catalog role déjà existant"

echo ""
echo "── Octroi du privilège CATALOG_MANAGE_CONTENT au catalog role..."
curl -sf -X PUT \
  "$POLARIS_URI/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/retailco_admin_role/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: $REALM" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' \
  > /dev/null && echo "✅ CATALOG_MANAGE_CONTENT accordé" || echo "⚠️  Grant déjà existant"

echo ""
echo "── Liaison du catalog role au principal-role 'service_admin' (root)..."
curl -sf -X PUT \
  "$POLARIS_URI/api/management/v1/principal-roles/service_admin/catalog-roles/${CATALOG_NAME}" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: $REALM" \
  -d '{"catalogRole": {"name": "retailco_admin_role"}}' \
  > /dev/null && echo "✅ catalog role lié à service_admin (root en hérite)" || echo "⚠️  Lien déjà existant"

echo ""
echo "=== Setup terminé ==="
echo "  Catalog          : $CATALOG_NAME"
echo "  Namespace        : retailco"
echo "  Catalog role     : retailco_admin_role (CATALOG_MANAGE_CONTENT)"
echo "  Base location    : s3a://warehouse (scheme compatible Spark S3A)"
echo "  MinIO interne    : $MINIO_ENDPOINT_INT"
echo "  OAuth2 clientId  : $CLIENT_ID"
