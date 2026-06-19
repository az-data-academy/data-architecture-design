# Lab J1 — Iceberg Masterclass

## Prérequis
```bash
./infra/switch_profile.sh iceberg
./infra/health-check.sh iceberg
```

## Labs

| Notebook | Durée | Position | Contenu |
|----------|-------|----------|---------|
| `lab_iceberg_format_versions.ipynb` | 45 min | J1 — après internals | Comparatif V1/V2/V3 : DELETE, Deletion Vectors, Variant, migration |
| `lab_iceberg_avance.ipynb` | 90 min | J1 — labs principaux | CoW/MoR, compaction, hidden partitioning, Z-ordering |
| `lab_polaris_avance.ipynb` | 60 min | J1 — labs principaux | Multi-tenant, OAuth2, fédération multi-catalog |

## Interfaces utiles
- JupyterLab : http://localhost:8888 (token: formation2024)
- MinIO Console : http://localhost:9001 (admin/minioadmin) — pour observer les fichiers
- Trino UI : http://localhost:8080 — pour les requêtes SQL

## Accès Trino via CLI
```bash
docker exec trino trino --server http://localhost:8080 --catalog polaris --schema retailco
```
