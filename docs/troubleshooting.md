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

## 11. Nessie RocksDB — permission denied au démarrage

```bash
# Si le volume RocksDB a des problèmes de permissions
docker volume rm formation-nessie-rocksdb
docker compose --profile infra up -d nessie
```

## 12. Polaris H2 — base de données verrouillée

```bash
# Si Polaris refuse de démarrer après un crash (lock file H2)
docker volume rm formation-polaris-data
docker compose --profile infra up -d polaris
# Puis relancer init.sh pour recréer les namespaces
./infra/init.sh
```

## 13. Jupyter — lent au 1er démarrage

Normal : pip install prend 3-8 min la 1re fois.
Le cache pip (volume formation-pip-cache) accélère les redémarrages suivants.
```bash
# Voir la progression
docker logs -f jupyter | grep -E "Successfully|ERROR"
```

## 14. MLflow — experiments non visibles

Si MLflow a été recréé, la DB SQLite est dans le volume mlflow-db.
```bash
docker volume ls | grep mlflow
# Si le volume n'existe pas, créer depuis les logs
docker compose --profile mlops up -d mlflow
```

## 15. MinIO — image non trouvée sur Docker Hub

MinIO a arrêté toutes les images Docker Hub et Quay.io en octobre 2025. On utilise Chainguard.

```bash
# Si vous avez une ancienne référence minio/minio:RELEASE.* → remplacer par :
image: cgr.dev/chainguard/minio:latest

# Pour le client mc :
image: cgr.dev/chainguard/minio-client:latest

# Vérifier que l'image est accessible
docker pull cgr.dev/chainguard/minio:latest
```

## 16. Healthcheck MinIO Chainguard — mc ready local

L'image Chainguard est distroless. Le binaire `mc` est présent dans l'image serveur.
```bash
# Test manuel depuis l'extérieur
docker exec minio mc ready local

# Si mc ready échoue, utiliser la commande native :
test: ["CMD-SHELL", "mc alias set local http://localhost:9000 minioadmin minioadmin && mc ready local"]
```
