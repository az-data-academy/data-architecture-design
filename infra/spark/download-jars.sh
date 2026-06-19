#!/bin/sh
# =============================================================================
# download-jars.sh — Télécharge les JARs Spark dans le volume spark-jars
# =============================================================================
set -e
JARS_DIR=/opt/spark-jars
mkdir -p "$JARS_DIR"
BASE="https://repo1.maven.org/maven2"

download() {
  local name="$1" url="$2"
  if [ -f "$JARS_DIR/$name" ]; then
    echo "  [SKIP] $name déjà présent"
  else
    echo "  [DL]   $name ..."
    wget -q "$url" -O "$JARS_DIR/$name" && echo "  [OK]   $name" || { echo "  [ERR]  $name"; exit 1; }
  fi
}

echo "=== Téléchargement des JARs ==="

download "iceberg-spark-runtime-3.5_2.12-1.11.0.jar" \
  "$BASE/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar"

download "iceberg-aws-bundle-1.11.0.jar" \
  "$BASE/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar"

download "nessie-spark-extensions-3.5_2.12-0.105.3.jar" \
  "$BASE/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.105.3/nessie-spark-extensions-3.5_2.12-0.105.3.jar"

download "hadoop-aws-3.3.4.jar" \
  "$BASE/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

download "aws-java-sdk-bundle-1.12.262.jar" \
  "$BASE/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

echo ""
ls -lh "$JARS_DIR"
echo "✅ spark-jars-init terminé"
