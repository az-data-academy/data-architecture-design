# Jour 2 — Versioning Git-like avec Nessie, CI/CD pour la donnée, Feature Store & MLOps

**Formation Data Lakehouse — AZ-DATA Academy**

---

## Objectifs pédagogiques

À l'issue de cette journée, vous serez capable de :

- Expliquer le modèle de branches/commits de Nessie et le différencier du modèle de snapshots Iceberg simple
- Mettre en place un flux de type Git pour la donnée : créer une branche, isoler un changement, le valider, le fusionner
- Diagnostiquer et corriger un incident de données en isolation, sans jamais exposer l'erreur sur la branche de production
- Construire un Feature Store avec Feast adossé à des tables Iceberg/Nessie
- Tracer des expérimentations ML avec MLflow, depuis l'entraînement jusqu'au Model Registry

---

## Module 1 — Pourquoi Nessie ? Le modèle Git pour la donnée

### 1.1 Limite du catalogue REST Iceberg simple

Avec Polaris (vu hier), chaque table a **une seule ligne de temps** : une suite linéaire de snapshots. C'est suffisant pour du time travel, mais ça ne permet pas de travailler en **isolation** : si deux personnes modifient la même table en parallèle, ou si vous voulez tester une correction de données avant de l'exposer aux utilisateurs de production, vous n'avez pas de mécanisme natif pour ça.

### 1.2 Le modèle Nessie

Nessie ajoute une couche de **versioning multi-table de type Git** au-dessus d'Iceberg :

- Une **branche** (`branch`) est un pointeur mobile vers un commit, exactement comme dans Git.
- Un **commit** capture l'état de **toutes les tables** du catalogue à cet instant — pas une seule table comme un snapshot Iceberg isolé.
- `main` est la branche par défaut, généralement assimilée à la production.
- On peut créer des branches de travail (`feat/...`, `fix/...`), y committer des changements en isolation totale, puis les **merger** vers `main` une fois validés.

C'est le même mental model que pour le code source — sauf qu'ici, ce qu'on versionne, ce sont des **tables de données entières**, à l'échelle du commit.

### 1.3 Différence clé avec un simple snapshot Iceberg

| | Snapshot Iceberg seul | Branche Nessie |
|---|---|---|
| Granularité | Une table | Tout le catalogue (toutes tables) |
| Isolation multi-table | ❌ | ✅ — un commit Nessie peut toucher plusieurs tables atomiquement |
| Branches de travail nommées | ❌ | ✅ |
| Merge avec détection de conflit | ❌ | ✅ |
| Rollback simple | Via time travel sur 1 table | Via réassignation de branche (toutes tables) |

---

## Module 2 — Manipuler Nessie : l'API REST v2

### 2.1 Authentification et configuration Spark

Comme pour Polaris, on configure un catalogue Spark dédié pointant vers Nessie :

```python
spark = (SparkSession.builder
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.defaultCatalog", "nessie")
    .getOrCreate())
```

Le paramètre `spark.sql.catalog.nessie.ref` fixe la branche active pour cette session Spark — toutes les lectures/écritures via `nessie.retailco.*` se font sur cette branche tant qu'elle n'est pas changée.

### 2.2 Les opérations REST fondamentales

Nessie expose une API REST v2 complète. Voici les opérations couvertes en lab, avec les pièges identifiés en pratique :

**Lister les branches :**
```python
GET /api/v2/trees
# → {"references": [{"type": "BRANCH", "name": "main", "hash": "..."}]}
```

**Créer une branche** — point d'attention important : `name` et `type` sont des **query parameters**, pas des champs du corps de la requête. Le corps ne contient que la référence **source** (généralement `main`, avec son hash actuel) :

```python
POST /api/v2/trees?name=feat/nessie-demo&type=BRANCH
Body: {"type": "BRANCH", "name": "main", "hash": "<hash_de_main>"}
```

**Consulter l'historique de commits d'une branche** (pas `/log`, qui n'existe pas en v2) :
```python
GET /api/v2/trees/main/history
# → {"logEntries": [...], "hasMore": false}
```

**Réassigner une branche** (rollback) — utilise `PUT`, pas un endpoint `/assign` dédié, et exige le **hash courant** de la branche dans le chemin (mécanisme de concurrence optimiste qui empêche un rollback aveugle si quelqu'un d'autre a déjà bougé la branche entre-temps) :
```python
hash_courant = nessie('GET', '/trees/main')['reference']['hash']
PUT /api/v2/trees/main@{hash_courant}
Body: {"type": "BRANCH", "name": "main", "hash": "<hash_cible>"}
```

**Fusionner une branche dans une autre**, même logique de hash courant requis :
```python
POST /api/v2/trees/main@{hash_courant}/history/merge
Body: {"fromRefName": "fix/silver-ttc-correction", "fromHash": "<hash_source>"}
```

### 2.3 Encodage des noms de branches

Les noms de branches contenant des `/` (convention `feat/...`, `fix/...`, courante en Git) doivent être **URL-encodés** dans le chemin REST, sinon Nessie interprète le `/` comme un séparateur de chemin et casse la requête :

```python
from urllib.parse import quote

def nessie_ref(branch_name: str) -> str:
    return quote(branch_name, safe='')
```

---

## Module 3 — Cas d'usage : isoler et corriger un incident de données

C'est le scénario du lab `lab_nessie_cicd_data`, qui simule une situation réaliste d'ingénierie de données.

### 3.1 Le scénario

Une transformation `silver_transactions` calcule un montant TTC erroné (formule de calcul de TVA incorrecte appliquée par erreur). Le bug est déjà en production sur `main`. La procédure de correction, sans Nessie, impliquerait de modifier directement la table de production — risqué si la correction elle-même contient une erreur.

### 3.2 La procédure avec Nessie

1. **Créer une branche de fix** depuis l'état actuel de `main` :
   ```python
   current_hash = nessie('GET', '/trees/main')['reference']['hash']
   nessie('POST', f'/trees?name={nessie_ref("fix/silver-ttc-correction")}&type=BRANCH',
          {'type': 'BRANCH', 'name': 'main', 'hash': current_hash})
   ```

2. **Basculer Spark sur cette branche** et appliquer le fix en isolation :
   ```python
   spark.conf.set("spark.sql.catalog.nessie.ref", "fix/silver-ttc-correction")
   spark.sql("""
       INSERT INTO nessie.retailco.silver_transactions
       SELECT *, ROUND(total_amount_xof * 1.18, 0) AS total_ttc_xof_corrige
       FROM nessie.retailco.silver_transactions
       WHERE total_ttc_xof < total_amount_xof
   """)
   ```
   Pendant cette phase, **les utilisateurs interrogeant `main` ne voient strictement rien du fix en cours** — c'est l'isolation Nessie en action.

3. **Valider la correction** par un test de qualité sur la branche de fix (comparaison avant/après, vérification d'absence d'anomalie résiduelle).

4. **Fusionner vers `main`** une fois la validation passée :
   ```python
   fix_hash = nessie('GET', f'/trees/{nessie_ref("fix/silver-ttc-correction")}')['reference']['hash']
   main_hash = nessie('GET', '/trees/main')['reference']['hash']
   nessie('POST', f'/trees/main@{main_hash}/history/merge',
          {'fromRefName': 'fix/silver-ttc-correction', 'fromHash': fix_hash})
   ```

### 3.3 Idempotence : un réflexe de production

Un point appris en pratique lors des tests répétés de ce pipeline : si le notebook est relancé plusieurs fois (ce qui arrive en formation, et en développement en général), la création de branche échoue en `409 Conflict` car elle existe déjà. Un pipeline robuste doit gérer ce cas explicitement plutôt que de planter :

```python
resp = nessie('POST', f'/trees?name={nessie_ref(branch)}&type=BRANCH',
               {'type': 'BRANCH', 'name': 'main', 'hash': current_hash},
               ignore_conflict=True)
if resp.get('errorCode') == 'REFERENCE_ALREADY_EXISTS':
    # Réaligner la branche existante sur l'état actuel de main,
    # pour garantir un état propre et reproductible à chaque exécution
    ...
```

Ce réflexe d'idempotence est directement transposable à n'importe quel pipeline CI/CD de données : un job qui peut être relancé sans effet de bord est un job fiable.

---

## Module 4 — Feature Store avec Feast

### 4.1 Pourquoi un Feature Store ?

En Machine Learning, une **feature** est une variable calculée à partir des données brutes, utilisée comme entrée d'un modèle (ex : "nombre de transactions du client dans les 30 derniers jours"). Le problème classique : la même feature doit être calculée **de façon identique** à l'entraînement (sur un historique complet, en batch) et à l'inférence (en production, en temps réel ou quasi temps réel) — un écart entre ces deux calculs (le *training-serving skew*) est une cause fréquente de dégradation silencieuse de modèles en production.

Un Feature Store comme **Feast** résout ce problème en centralisant :
- la **définition** des features (un seul endroit où la logique de calcul est déclarée)
- l'**offline store** (historique complet, pour l'entraînement) — ici, nos tables Iceberg/Nessie
- l'**online store** (dernière valeur connue par entité, accès faible latence pour l'inférence) — ici, Redis

### 4.2 Architecture dans notre stack

```
Tables Iceberg/Nessie (offline store)
        │
        │  feast apply / materialize
        ▼
   Feast Registry
        │
        ▼
   Redis (online store) ──→ Service d'inférence (faible latence)
```

Le fichier `feature_store.yaml` déclare la connexion au store en ligne :

```yaml
project: retailco_features
provider: local
online_store:
    type: redis
    connection_string: "redis:6379"
```

### 4.3 Définir une feature view

```python
from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64

customer_recency = FeatureView(
    name="customer_recency_features",
    entities=["customer_id"],
    schema=[
        Field(name="days_since_last_purchase", dtype=Int64),
        Field(name="avg_basket_xof", dtype=Float64),
    ],
    source=transactions_source,
    ttl=timedelta(days=90),
)
```

### 4.4 Le flux materialize

`feast materialize` synchronise les dernières valeurs de l'offline store (Iceberg) vers l'online store (Redis), pour que les features soient disponibles à faible latence au moment de l'inférence :

```bash
feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
```

Ce mécanisme s'intègre naturellement avec Nessie : on peut matérialiser des features depuis une branche d'expérimentation (`feat/new-recency-feature`) sans jamais toucher aux features servies en production depuis `main`, en suivant exactement le même pattern d'isolation que pour la correction de données du Module 3.

---

## Module 5 — Tracking ML avec MLflow

### 5.1 Les trois piliers de MLflow

- **Tracking** : enregistrer les paramètres, métriques et artefacts de chaque run d'entraînement
- **Model Registry** : versionner les modèles entraînés, gérer leur cycle de vie (staging → production)
- **Model Serving** : exposer un modèle enregistré via une API d'inférence

### 5.2 Tracer un run d'entraînement

```python
import mlflow
import mlflow.sklearn

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("retailco_churn_prediction")

with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=100, max_depth=8)
    model.fit(X_train_s, y_train)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 8)
    mlflow.log_metric("accuracy", accuracy_score(y_test, model.predict(X_test_s)))

    mlflow.sklearn.log_model(
        model, name="model",
        registered_model_name="RetailCoChurnModel",
        input_example=X_test_s[:3],
    )
```

Notez `name="model"` plutôt que l'ancien paramètre positionnel `artifact_path` : MLflow 3.x a déprécié `artifact_path` au profit de `name`, qui s'aligne sur le nouveau concept de **Logged Model** introduit dans cette version majeure — un changement d'API à connaître si vous migrez un pipeline écrit pour MLflow 2.x.

### 5.3 Le Model Registry et le versioning

Chaque appel à `log_model` avec `registered_model_name` crée une nouvelle **version** du modèle dans le registre (version 1, 2, 3...). Pour charger une version précise :

```python
loaded_model = mlflow.sklearn.load_model("models:/RetailCoChurnModel/1")
```

En pratique de production, on préfère généralement utiliser un **alias** (`champion`, `challenger`, `latest`) plutôt qu'un numéro de version fixe, pour découpler le code de chargement du modèle de son cycle de versioning :

```python
mlflow.set_registered_model_alias("RetailCoChurnModel", "champion", version=3)
loaded_model = mlflow.sklearn.load_model("models:/RetailCoChurnModel@champion")
```

### 5.4 Boucler la boucle : Nessie + Feast + MLflow

L'architecture complète du jour relie les trois briques : les features sont calculées depuis des tables Iceberg versionnées par Nessie, matérialisées dans Feast pour l'inférence, et le modèle qui les consomme est tracé et versionné dans MLflow. Cette chaîne complète permet, en cas de dégradation d'un modèle en production, de retracer précisément : quelle version du modèle, entraînée sur quelles features, calculées depuis quel état exact des données source (quel commit Nessie).

---

## Synthèse J2

| Concept | À retenir |
|---|---|
| Branche Nessie | Pointeur mobile sur un commit multi-table, isolation totale entre branches |
| Merge avec hash courant | Concurrence optimiste — protège contre un rollback/merge aveugle |
| Idempotence de pipeline | Toujours gérer le cas « déjà exécuté » sans planter |
| Feature Store (Feast) | Centralise la définition + cohérence offline/online des features ML |
| Offline / Online store | Iceberg (historique complet) / Redis (faible latence, dernière valeur) |
| MLflow Tracking | Paramètres + métriques + artefacts par run |
| MLflow Model Registry | Versioning + alias de modèles, découplé du code consommateur |

## Pour aller plus loin

- Documentation Nessie : [projectnessie.org](https://projectnessie.org/)
- Documentation Feast : [docs.feast.dev](https://docs.feast.dev/)
- Documentation MLflow : [mlflow.org/docs](https://mlflow.org/docs/latest/index.html)
