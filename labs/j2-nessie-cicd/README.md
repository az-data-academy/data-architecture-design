# Lab J2-A — Nessie CI/CD Data

## Prérequis
```bash
./infra/switch_profile.sh cicd
./infra/health-check.sh cicd
```

## Labs

| Notebook | Durée | Contenu |
|----------|-------|---------|
| `lab_nessie_cicd_data.ipynb` | 70 min | Branching, dbt workflow, merge, rollback incident |

## Interfaces
- Nessie API : http://localhost:19120
- Airflow UI : http://localhost:8089 (admin/admin)
- JupyterLab : http://localhost:8888

## Inspecter les branches Nessie
```bash
curl -s http://localhost:19120/api/v2/trees | python3 -m json.tool
curl -s http://localhost:19120/api/v2/trees/main/log | python3 -m json.tool
```
