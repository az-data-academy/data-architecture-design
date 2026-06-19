# Gestion de l'image Jupyter custom

## Build de l'image

```bash
# Depuis la racine du repo
docker compose build jupyter

# Ou directement avec Docker
docker build -t formation-lakehouse-jupyter:1.0.0 \
  -f infra/jupyter/Dockerfile .
```

## Contenu de l'image

| Composant | Version | Embarqué |
|-----------|---------|---------|
| PySpark | 3.5.0 | Base image |
| Iceberg Spark Runtime | 1.11.0 | /opt/spark-jars/ |
| Nessie Spark Extensions | 0.105.3 | /opt/spark-jars/ |
| Hadoop AWS | 3.3.4 | /opt/spark-jars/ |
| AWS Java SDK Bundle | 1.12.262 | /opt/spark-jars/ |
| Python packages | 249 (requirements.txt) | pip |
| spark-defaults.conf | Polaris + Nessie + MinIO | /usr/local/spark/conf/ |

## Avantage vs image de base

| | Image de base | Image custom |
|--|---------------|--------------|
| Démarrage | 3-5 min (pip install) | ~10 sec |
| Offline | ❌ nécessite internet | ✅ air-gapped |
| Reproductibilité | Variable (versions pip) | Fixe (build figé) |

## Mettre à jour les dépendances

1. Modifier `requirements.in`
2. Régénérer : `uv pip compile requirements.in --python-version 3.11 -o requirements.txt`
3. Rebuilder : `docker compose build jupyter`

## Pusher sur un registry (optionnel)

```bash
docker tag formation-lakehouse-jupyter:1.0.0 votre-registry/formation-lakehouse-jupyter:1.0.0
docker push votre-registry/formation-lakehouse-jupyter:1.0.0
```
