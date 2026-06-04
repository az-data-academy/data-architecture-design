# Lab J3 — Gouvernance & Lineage

## Prérequis
```bash
./infra/switch_profile.sh governance
./infra/health-check.sh governance
```
> OpenMetadata prend 2-3 min à démarrer. Attendre que le healthcheck soit "healthy".

## Labs

| Notebook | Durée | Contenu |
|----------|-------|---------|
| `lab_openmetadata_lineage.ipynb` | 70 min | Catalogage Iceberg, lineage, data contracts, RGPD CoW/MoR |

## Interfaces
- OpenMetadata : http://localhost:8585 (admin/admin)
- JupyterLab : http://localhost:8888
- MinIO Console : http://localhost:9001

## Connexion OpenMetadata → Iceberg via Polaris
Voir le notebook — la configuration est pré-remplie dans les cellules de setup.
