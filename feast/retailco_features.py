"""
RetailCo — Définitions des features pour le Feature Store (Feast)
Features calculées depuis la couche Silver Iceberg sur MinIO

Entités :
  - customer_id (str) : identifiant client unique
  - store_id (str) : identifiant magasin

Feature Views :
  - customer_rfm_features : Recency, Frequency, Monetary
  - customer_churn_features : indicateurs de risque de churn
  - store_performance_features : KPIs magasin agrégés
"""

from datetime import timedelta
from feast import (
    Entity, Feature, FeatureView, FileSource,
    ValueType, Field, FeatureService
)
from feast.types import Float32, Float64, Int64, String

# ── Entités ──────────────────────────────────────────────────────────────────
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    description="Identifiant unique du client RetailCo",
)

store = Entity(
    name="store_id",
    join_keys=["store_id"],
    description="Identifiant unique du magasin RetailCo",
)

# ── Sources (fichiers Parquet pré-calculés depuis Iceberg) ───────────────────
# En production : remplacer FileSource par SparkOfflineStore + Iceberg

customer_rfm_source = FileSource(
    name="customer_rfm_source",
    path="s3://feast/customer_rfm.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_churn_source = FileSource(
    name="customer_churn_source",
    path="s3://feast/customer_churn.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

store_perf_source = FileSource(
    name="store_perf_source",
    path="s3://feast/store_performance.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# ── Feature Views ─────────────────────────────────────────────────────────────
customer_rfm_features = FeatureView(
    name="customer_rfm_features",
    entities=[customer],
    ttl=timedelta(days=30),
    schema=[
        Field(name="recency_days",    dtype=Int64,   description="Jours depuis le dernier achat"),
        Field(name="frequency_30d",   dtype=Int64,   description="Nb achats sur les 30 derniers jours"),
        Field(name="monetary_90d",    dtype=Float64, description="CA généré sur 90 jours (FCFA)"),
        Field(name="avg_basket_size", dtype=Float64, description="Panier moyen (FCFA)"),
        Field(name="return_rate",     dtype=Float32, description="Taux de retour (0-1)"),
    ],
    source=customer_rfm_source,
    description="Features RFM client — recency, frequency, monetary",
)

customer_churn_features = FeatureView(
    name="customer_churn_features",
    entities=[customer],
    ttl=timedelta(days=7),
    schema=[
        Field(name="days_since_last_purchase", dtype=Int64,   description="Inactivité en jours"),
        Field(name="purchase_trend_30_60",     dtype=Float32, description="Ratio achats 30j vs 60j"),
        Field(name="channel_diversity_score",  dtype=Float32, description="Score diversité canaux (0-1)"),
        Field(name="churn_risk_score",         dtype=Float32, description="Score de risque churn (0-1)"),
        Field(name="segment",                  dtype=String,  description="Segment : champion/loyal/at-risk/lost"),
    ],
    source=customer_churn_source,
    description="Features de risque de churn client",
)

store_performance_features = FeatureView(
    name="store_performance_features",
    entities=[store],
    ttl=timedelta(days=7),
    schema=[
        Field(name="ca_7d",              dtype=Float64, description="CA 7 derniers jours"),
        Field(name="ca_vs_lw_pct",       dtype=Float32, description="Évolution vs semaine précédente"),
        Field(name="nb_transactions_7d", dtype=Int64,   description="Nb transactions 7 jours"),
        Field(name="avg_basket_7d",      dtype=Float64, description="Panier moyen 7 jours"),
        Field(name="return_rate_7d",     dtype=Float32, description="Taux de retour 7 jours"),
    ],
    source=store_perf_source,
    description="Features de performance magasin (hebdomadaires)",
)

# ── Feature Services ─────────────────────────────────────────────────────────
churn_prediction_service = FeatureService(
    name="churn_prediction_service",
    features=[
        customer_rfm_features[["recency_days", "frequency_30d", "monetary_90d", "return_rate"]],
        customer_churn_features[["days_since_last_purchase", "purchase_trend_30_60", "churn_risk_score"]],
    ],
    description="Features pour le modèle de prédiction de churn client",
)

recommendation_service = FeatureService(
    name="recommendation_service",
    features=[
        customer_rfm_features[["frequency_30d", "avg_basket_size", "return_rate"]],
        customer_churn_features[["segment"]],
    ],
    description="Features pour le moteur de recommandation produit",
)
