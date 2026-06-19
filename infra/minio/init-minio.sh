#!/bin/sh
# =============================================================================
# init-minio.sh — Initialisation MinIO : buckets + upload données RetailCo
# =============================================================================
set -e

MINIO_URL="http://minio:9000"
ALIAS="local"

echo "=== MinIO Init ==="
echo "Attente de MinIO..."
until mc alias set "$ALIAS" "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  sleep 2
done
echo "✅ Connecté à MinIO"

echo ""
echo "── Création des buckets ──────────────────────────────────"
for bucket in bronze silver gold warehouse mlflow feast; do
  mc mb --ignore-existing "$ALIAS/$bucket"
  echo "  ✅ s3://$bucket"
done

echo ""
echo "── Upload données RetailCo → s3://bronze/retailco/ ───────"

# Vérifier que les fichiers existent
if [ ! -f /data/retailco_transactions.csv ]; then
  echo "❌ ERREUR : /data/retailco_transactions.csv introuvable"
  exit 1
fi

mc cp /data/retailco_transactions.csv        "$ALIAS/bronze/retailco/retailco_transactions.csv"
echo "  ✅ retailco_transactions.csv (1000 lignes)"

mc cp /data/retailco_transactions_sample.csv "$ALIAS/bronze/retailco/retailco_transactions_sample.csv"
echo "  ✅ retailco_transactions_sample.csv (30 lignes)"

mc cp /data/retailco_transactions_clean.csv  "$ALIAS/bronze/retailco/retailco_transactions_clean.csv"
echo "  ✅ retailco_transactions_clean.csv"

echo ""
echo "── Vérification ──────────────────────────────────────────"
mc ls "$ALIAS/bronze/retailco/"

echo ""
echo "✅ MinIO initialisé — données RetailCo prêtes sur s3://bronze/retailco/"
