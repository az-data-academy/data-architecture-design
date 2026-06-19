# Lab J2-B — Feature Store & MLflow

## Prérequis
```bash
./infra/switch_profile.sh mlops
./infra/health-check.sh mlops
```

## Labs

| Notebook | Durée | Contenu |
|----------|-------|---------|
| `lab_feast_iceberg_nessie.ipynb` | 90 min | Feast offline/online, feature skew diagnostic, Nessie isolation |
| `lab_mlflow_pipeline.ipynb` | 75 min | Features → entraînement → MLflow → serving reproductible |

## Interfaces
- MLflow UI : http://localhost:5000
- Feast Server : http://localhost:6566
- JupyterLab : http://localhost:8888

## Init Feast
```bash
cd /home/jovyan/feast
feast apply
feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
```
