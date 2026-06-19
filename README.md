# Formation Data Architecture — Lakehouse, Open-Source & MLOps

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/VOTRE-ORG/formation-data-lakehouse?quickstart=1)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Stack complète de formation sur les architectures data Lakehouse avec Apache Iceberg, MinIO, Apache Polaris, Apache Nessie, Trino, Feast et MLflow.

---

## Démarrage rapide

### Option 1 — GitHub Codespaces (recommandé)

Cliquez sur le badge ci-dessus. La stack démarre automatiquement. Aucune installation locale requise.

Accès une fois démarré :
- **JupyterLab** → http://localhost:8888 · token : `formation2024`
- **MinIO Console** → http://localhost:9001 · admin/minioadmin
- **Trino UI** → http://localhost:8080
- **Polaris API** → http://localhost:8181
- **Nessie API** → http://localhost:19120

### Option 2 — Docker Compose local

```bash
# Prérequis : Docker Desktop + 16 Go RAM disponibles
git clone https://github.com/VOTRE-ORG/formation-data-lakehouse
cd formation-data-lakehouse

# Démarrer la stack de base
docker compose --profile infra up -d

# Initialiser les buckets, catalogs et données
./infra/init.sh
```

---

## Structure du repo

```
formation-data-lakehouse/
├── README.md                          ← Ce fichier
├── docker-compose.yml                 ← Stack complète, 6 profils
├── .env.example                       ← Variables d'environnement
│
├── .devcontainer/
│   └── devcontainer.json              ← Config GitHub Codespaces
│
├── infra/
│   ├── init.sh                        ← Initialisation complète (idempotent)
│   ├── switch_profile.sh              ← Changement d'atelier
│   ├── health-check.sh                ← Vérification avant lab
│   ├── trino/
│   │   ├── catalog/polaris.properties ← Catalog Iceberg via Polaris
│   │   ├── catalog/nessie.properties  ← Catalog Iceberg via Nessie
│   │   └── config.properties
│   ├── spark/
│   │   └── spark-defaults.conf        ← Iceberg + S3 + Nessie
│   ├── postgres/
│   │   └── init-retailco.sql          ← Init BD démo CDC
│   └── debezium-connector.json        ← Config connecteur CDC
│
├── data/
│   ├── retailco_transactions_sample.csv  ← 30 lignes avec anomalies
│   └── generate_retailco_data.py         ← Générateur Faker (N lignes)
│
├── labs/
│   ├── j1-iceberg-masterclass/
│   │   ├── README.md
│   │   ├── lab_iceberg_format_versions.ipynb ← V1 vs V2 vs V3 — DELETE, DVs, Variant, migration
│   │   ├── lab_iceberg_avance.ipynb          ← CoW/MoR, compaction, hidden partitioning
│   │   └── lab_polaris_avance.ipynb          ← Multi-tenant, OAuth2, fédération
│   ├── j2-nessie-cicd/
│   │   ├── README.md
│   │   └── lab_nessie_cicd_data.ipynb    ← Branching, dbt, merge, rollback
│   ├── j2-feature-store/
│   │   ├── README.md
│   │   ├── lab_feast_iceberg_nessie.ipynb ← Feature Store end-to-end
│   │   └── lab_mlflow_pipeline.ipynb      ← Features → MLflow → serving
│   └── j3-governance/
│       ├── README.md
│       └── lab_openmetadata_lineage.ipynb ← Catalogage + lineage
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                   ← Profils Polaris + Nessie
│   └── models/
│       ├── silver/silver_transactions.sql
│       └── gold/gold_ca_by_store_week.sql
│
├── feast/
│   ├── feature_store.yaml             ← Config Feature Store
│   └── retailco_features.py           ← Définitions features (churn, RFM)
│
└── docs/
    ├── setup.md                       ← Guide de démarrage détaillé
    ├── stack-architecture.md          ← Schéma de la stack
    └── troubleshooting.md             ← 10 problèmes fréquents
```

---

## Profils d'ateliers

La stack est segmentée par atelier. Seuls les services nécessaires tournent à la fois.

| Profil | Atelier | Commande | RAM ~|
|--------|---------|----------|------|
| `infra` | Base (toujours actif) | `docker compose --profile infra up -d` | 6 Go |
| `iceberg` | J1 — Labs Iceberg avancé | `./infra/switch_profile.sh iceberg` | 14 Go |
| `cicd` | J2-A — Nessie CI/CD Data | `./infra/switch_profile.sh cicd` | 12 Go |
| `mlops` | J2-B — Feature Store + MLflow | `./infra/switch_profile.sh mlops` | 11 Go |
| `governance` | J3 — Gouvernance | `./infra/switch_profile.sh governance` | 9 Go |
| `cdc` | Démo CDC (formateur) | `./infra/switch_profile.sh cdc` | 11 Go |

> **Les volumes MinIO et Nessie sont partagés entre tous les profils.**
> Les tables Iceberg créées en J1 sont accessibles en J2 et J3 sans réimport.

---

## Stratégie de branches

| Branche | Contenu | Accès |
|---------|---------|-------|
| `main` | Labs avec TODO — branche des participants | Public |
| `solutions` | Notebooks complets avec corrections | Formateur |
| `feat/nessie-demo` | Point de départ du lab Nessie CI/CD | Public |
| `incident/*` | Pipelines pré-cassés pour les labs incidents | Formateur |

---

## Stack technique

| Composant | Version | Rôle | Port |
|-----------|---------|------|------|
| Apache Iceberg | 1.11.0 | Table format — V1/V2/V3 (lab comparatif) | — |
| MinIO | RELEASE.2024-01-16 | Stockage objet S3 | 9000/9001 |
| Apache Polaris | latest | Iceberg REST Catalog | 8181 |
| Apache Nessie | 0.77.1 | Git for Data | 19120 |
| Trino | 435 | Moteur SQL | 8080 |
| Apache Spark | 3.5.0 | Traitement distribué | 8085/7077 |
| Dremio OSS | 26.0.5 | Moteur SQL lakehouse + UI visuelle | 9047/31010/45678 |
| Apache Airflow | 2.8.0 | Orchestration | 8089 |
| dbt-trino | latest | Transformation SQL | — |
| Feast | 0.38.0 | Feature Store | 6566 |
| MLflow | 2.10.0 | Tracking ML | 5000 |
| Redis | 7 | Online feature store | 6379 |
| OpenMetadata | 1.3.1 | Catalogue & lineage | 8585 |
| Apache Kafka | 7.5.0 | Streaming (démo CDC) | 9092 |
| Debezium | 2.5 | CDC Postgres (démo) | 8083 |

---

## Troubleshooting rapide

```bash
# Vérifier l'état des services
./infra/health-check.sh [profil]

# Voir les logs d'un service
docker compose logs -f <service>

# Réinitialiser un service sans perdre les données
docker compose restart <service>

# Reset complet (⚠️ efface les données)
docker compose --profile infra down -v

# Voir les branches Nessie
curl -s http://localhost:19120/api/v2/trees | python3 -m json.tool
```

Voir [docs/troubleshooting.md](docs/troubleshooting.md) pour les 10 problèmes fréquents.

---

## Licence

MIT — Libre d'utilisation pour la formation et les projets clients.
