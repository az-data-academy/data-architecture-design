# Troubleshooting — 10 problèmes fréquents

## 1. MinIO ne répond pas

```bash
docker compose logs minio | tail -20
docker compose restart minio
# Attendre le healthcheck : docker compose ps minio
```

## 2. Polaris retourne 404 sur /api/catalog/v1/config

Polaris met 30-60s à démarrer. Attendre le healthcheck.
```bash
docker compose ps polaris
# STATUS doit être "healthy"
```

## 3. Table Iceberg non trouvée dans Trino

```sql
-- Vérifier le catalog actif
SHOW CATALOGS;
-- Vérifier les namespaces
SHOW SCHEMAS FROM polaris;
-- Vérifier les tables
SHOW TABLES FROM polaris.retailco;
```

## 4. Erreur S3 / MinIO dans Spark

```python
# Vérifier la config S3a dans spark-defaults.conf
spark.conf.get("spark.hadoop.fs.s3a.endpoint")
# Doit retourner : http://minio:9000
```

## 5. Nessie — branche non trouvée

```bash
# Lister les branches
curl -s http://localhost:19120/api/v2/trees | python3 -m json.tool

# Créer une branche manuellement
curl -X POST http://localhost:19120/api/v2/trees \
  -H "Content-Type: application/json" \
  -d '{"name":"ma-branche","type":"BRANCH","hash":"<hash-main>"}'
```

## 6. JupyterLab — kernel mort

```bash
# Redémarrer le kernel depuis l'interface : Kernel → Restart Kernel
# Ou redémarrer le container
docker compose restart jupyter
```

## 7. Airflow — DAG non visible

```bash
# Vérifier les erreurs de parsing
docker exec airflow airflow dags list
docker exec airflow airflow dags list-import-errors
```

## 8. MLflow — erreur de connexion S3

```bash
# Vérifier les variables d'environnement
docker exec mlflow env | grep -E "AWS|MLFLOW"
# AWS_ACCESS_KEY_ID doit être minioadmin
# MLFLOW_S3_ENDPOINT_URL doit être http://minio:9000
```

## 9. OpenMetadata — page blanche au démarrage

OpenMetadata prend 2-3 minutes à démarrer (Elasticsearch + migration DB).
```bash
docker compose logs openmetadata | grep -E "started|error|Exception"
```

## 10. Debezium — connecteur en erreur

```bash
# Voir l'état du connecteur
curl -s http://localhost:8083/connectors/retailco-postgres-connector/status | python3 -m json.tool

# Recréer le connecteur
curl -X DELETE http://localhost:8083/connectors/retailco-postgres-connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @infra/debezium-connector.json
```
