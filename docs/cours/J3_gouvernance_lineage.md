# Jour 3 — Gouvernance, Lineage & Conformité des Données

**Formation Data Lakehouse — AZ-DATA Academy**

---

## Objectifs pédagogiques

À l'issue de cette journée, vous serez capable de :

- Expliquer pourquoi la gouvernance devient incontournable à mesure qu'un Lakehouse grandit
- Cataloguer automatiquement des tables Iceberg dans OpenMetadata via le connecteur Polaris
- Lire et interpréter un graphe de lineage de données, de la source à la table consommée
- Définir et faire respecter un **data contract** entre producteurs et consommateurs de données
- Choisir une stratégie de suppression (CoW vs MoR) adaptée à une exigence réglementaire de type RGPD

---

## Module 1 — Pourquoi la gouvernance, et pourquoi maintenant

### 1.1 Le problème de l'échelle

Les deux premiers jours ont construit une plateforme technique fonctionnelle : tables Iceberg, catalogue REST, versioning Git-like, feature store, tracking ML. Mais une plateforme technique seule ne répond pas à des questions essentielles dès qu'elle est partagée par plusieurs équipes :

- *Qui* a créé cette table, et *pourquoi* ?
- *D'où* viennent ces données — quelle est la chaîne de transformations depuis la source ?
- Si je modifie cette colonne, *qui* va casser en aval ?
- Cette table contient-elle des données personnelles, et sommes-nous en conformité avec les obligations légales de conservation/suppression ?

Sans réponse outillée à ces questions, une organisation data accumule de la dette invisible : tables orphelines, dépendances cachées, incidents de conformité découverts a posteriori.

### 1.2 Le rôle d'un catalogue de gouvernance

Contrairement à Polaris (catalogue **technique**, qui répond « où sont les fichiers de cette table »), un outil comme **OpenMetadata** est un catalogue de **gouvernance** : il répond aux questions de contexte métier, de qualité, de traçabilité et de conformité, en se connectant en lecture aux catalogues techniques (Polaris, bases de données, outils BI) pour en extraire et enrichir les métadonnées.

```
            ┌─────────────────────────────────────┐
            │           OpenMetadata               │
            │  (catalogue de gouvernance)           │
            │  - Lineage   - Data contracts         │
            │  - Ownership - Qualité                │
            └───────────────┬───────────────────────┘
                             │ se connecte à
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
          Polaris         Trino        (autres sources :
       (catalogue        (moteur SQL    bases de données,
        technique         fédéré)        outils BI...)
        Iceberg)
```

---

## Module 2 — Cataloguer Iceberg dans OpenMetadata

### 2.1 Le connecteur Iceberg/Polaris

OpenMetadata se connecte au catalogue Polaris via son endpoint REST Iceberg standard, et synchronise automatiquement :
- la liste des namespaces et tables
- le schéma de chaque table, avec ses types de colonnes
- les propriétés de partitionnement
- les statistiques de base (nombre de lignes, taille)

Cette synchronisation peut être lancée en CLI ou via un pipeline d'ingestion planifié (typiquement orchestré par Airflow, vu en filigrane le jour précédent pour le CI/CD de données).

### 2.2 Propriétés et tags

Une fois les tables cataloguées, on les enrichit avec du contexte métier qu'aucun système technique ne peut déduire seul :

```python
contract = {
    "updates": {
        "owner": "data-engineering@retailco.ci",
        "domain": "ventes",
        "pii_classification": "low",  # transactions.customer_id est pseudonymisé
        "retention_policy": "7_years_fiscal",
    },
    "removals": []
}

requests.post(
    f"{POLARIS}/v1/retailco/namespaces/retailco/properties",
    json=contract,
    headers={"Content-Type": "application/json"}
)
```

Point d'attention pratique : la mise à jour de propriétés de namespace via l'API REST Iceberg exige le **préfixe du catalogue** dans le chemin (`/v1/retailco/namespaces/...`, pas `/v1/namespaces/...`) — un piège déjà rencontré côté Polaris pur, qui se reproduit identique dès qu'on script des appels REST directs depuis un outil tiers.

---

## Module 3 — Lineage : tracer le voyage de la donnée

### 3.1 Pourquoi le lineage est structurellement difficile en Lakehouse

Dans un entrepôt de données classique avec un seul moteur SQL, le lineage peut souvent être déduit en analysant les requêtes SQL exécutées (`CREATE TABLE AS SELECT`, vues, procédures stockées). Dans un Lakehouse, c'est plus complexe : les transformations peuvent être exécutées par Spark, par Trino, par un job dbt, par un notebook Jupyter ad hoc — chacun avec sa propre façon d'exprimer une dépendance.

### 3.2 Construire le lineage manuellement vs automatiquement

OpenMetadata supporte deux approches complémentaires :

**Lineage déclaratif** (au niveau pipeline) : un job déclare explicitement ses entrées et sorties au moment de son exécution — utile pour des transformations complexes où l'analyse statique de requête échouerait (logique conditionnelle, jointures dynamiques).

**Lineage par analyse de requêtes** : OpenMetadata peut parser les requêtes SQL exécutées via Trino (capturées dans ses logs de requêtes) pour déduire automatiquement les dépendances `table source → table cible`.

### 3.3 Lire un graphe de lineage

Un graphe de lineage typique pour notre pipeline RetailCo :

```
retailco_transactions.csv (MinIO, raw)
        │
        ▼  Spark — ingestion brute
polaris.retailco.transactions (bronze)
        │
        ▼  Spark — nettoyage + dédoublonnage
nessie.retailco.silver_transactions
        │
        ├──▼  Agrégation quotidienne
        │  polaris.analytics.sales_summary
        │
        └──▼  Feast — feature engineering
           customer_recency_features (offline)
                  │
                  ▼  materialize
           Redis (online store)
```

L'intérêt opérationnel concret : si une anomalie est détectée dans `sales_summary`, le lineage permet de remonter immédiatement la chaîne jusqu'à `silver_transactions`, puis à la source brute, sans avoir à interroger chaque équipe une par une — un gain de temps direct en situation d'incident.

### 3.4 Lineage et impact analysis

La vue symétrique du lineage est l'**impact analysis** : avant de modifier le schéma de `silver_transactions` (par exemple, renommer une colonne), on interroge le graphe en aval pour identifier toutes les tables et tous les modèles ML qui en dépendent — `sales_summary`, `customer_recency_features`, et potentiellement le modèle `RetailCoChurnModel` vu hier. C'est l'outillage qui rend un changement de schéma **gouvernable** plutôt que risqué.

---

## Module 4 — Data Contracts

### 4.1 Le principe

Un **data contract** est un accord explicite, vérifiable mécaniquement, entre une équipe productrice de données et ses consommateurs. Il formalise :
- le schéma garanti (colonnes, types, contraintes de nullabilité)
- les règles de qualité attendues (ex : `total_amount_xof >= 0`, `transaction_id` unique)
- la fréquence de mise à jour attendue (SLA de fraîcheur)
- la politique de gestion des breaking changes (versioning, période de dépréciation)

### 4.2 Pourquoi formaliser plutôt que documenter

La différence entre un data contract et une simple documentation : le contrat est **vérifié automatiquement**, généralement dans le pipeline CI/CD qui produit la donnée. Un changement de schéma qui viole le contrat doit faire échouer le build, exactement comme un test unitaire fait échouer un build de code applicatif qui casse une interface.

### 4.3 Exemple de vérification de contrat

```python
def valider_contrat_qualite(df, contrat):
    erreurs = []

    # Schéma : colonnes attendues présentes
    colonnes_manquantes = set(contrat["colonnes_requises"]) - set(df.columns)
    if colonnes_manquantes:
        erreurs.append(f"Colonnes manquantes : {colonnes_manquantes}")

    # Règle métier : montant non négatif
    n_negatifs = df.filter("total_amount_xof < 0").count()
    if n_negatifs > 0:
        erreurs.append(f"{n_negatifs} montant(s) négatif(s) détecté(s)")

    # Unicité de la clé
    n_doublons = df.count() - df.dropDuplicates(["transaction_id"]).count()
    if n_doublons > 0:
        erreurs.append(f"{n_doublons} transaction_id dupliqué(s)")

    return erreurs

erreurs = valider_contrat_qualite(df, contrat_transactions)
print(f"✅ Test qualité : 0 erreur — PASSED" if not erreurs
      else f"❌ Test qualité : {len(erreurs)} erreur(s) — FAILED")
```

Cette logique de validation est typiquement exécutée comme une étape de pipeline (Airflow, ou une tâche dans le merge Nessie du jour précédent), avant qu'une branche de correction de données ne soit autorisée à fusionner vers `main`.

---

## Module 5 — Conformité RGPD/UEMOA et choix CoW vs MoR

### 5.1 Le droit à l'oubli comme exigence technique

Les réglementations de protection des données (RGPD en zone UE, et les législations nationales équivalentes en zone UEMOA, ex. la loi ivoirienne sur la protection des données à caractère personnel) imposent un **droit à l'effacement** : un client peut demander la suppression de ses données personnelles, et l'organisation doit pouvoir s'exécuter dans un délai contraint.

Sur une table Iceberg de plusieurs millions de lignes, supprimer les enregistrements d'un seul `customer_id` est exactement le scénario qui détermine le choix entre CoW et MoR vu au Jour 1 :

| Approche | Comportement pour une suppression RGPD ciblée | Implication |
|---|---|---|
| Copy-on-Write | Réécrit entièrement chaque fichier Parquet contenant au moins une ligne du client concerné | Coût élevé si les lignes sont dispersées dans beaucoup de fichiers ; mais garantit que **la donnée n'existe physiquement plus nulle part** dès la fin de l'opération |
| Merge-on-Read | Écrit un delete file référençant les lignes à exclure ; les octets originaux restent sur le stockage tant qu'une compaction n'a pas eu lieu | Suppression logique immédiate et peu coûteuse, mais la donnée brute **persiste physiquement** jusqu'à compaction — point de vigilance pour une obligation stricte d'effacement physique |

### 5.2 La bonne pratique : MoR + compaction forcée

Pour concilier la performance d'écriture de MoR avec l'obligation d'effacement physique, le pattern recommandé est :
1. Appliquer la suppression en MoR (rapide, faible impact opérationnel immédiat)
2. Déclencher une procédure `rewrite_data_files` **ciblée sur les partitions concernées**, dans un délai compatible avec l'obligation légale (souvent 30 jours)
3. Suivre `expire_snapshots` pour purger les anciens snapshots qui référenceraient encore les fichiers pré-suppression

```sql
-- Étape 1 : suppression logique (MoR, immédiate)
DELETE FROM nessie.retailco.transactions WHERE customer_id = 'cust-00614';

-- Étape 2 : compaction pour effacement physique
CALL nessie.system.rewrite_data_files(
    table => 'retailco.transactions',
    options => map('target-file-size-bytes', '134217728')
);

-- Étape 3 : purge des anciens snapshots qui référencent encore les données pré-suppression
CALL nessie.system.expire_snapshots(
    table => 'retailco.transactions',
    older_than => TIMESTAMP_LITERAL  -- au-delà de la fenêtre légale de rétention
);
```

### 5.3 Documenter la conformité dans le data contract

Le choix CoW/MoR et la procédure de purge associée font naturellement partie du data contract d'une table contenant des données personnelles — c'est un exemple concret où la gouvernance documentaire (Module 4) et l'architecture technique (Jour 1) se rejoignent directement.

---

## Synthèse J3

| Concept | À retenir |
|---|---|
| Catalogue de gouvernance vs catalogue technique | OpenMetadata répond au « qui/pourquoi/conforme », Polaris au « où sont les fichiers » |
| Lineage | Graphe de dépendances source → transformation → consommation, essentiel en incident et en impact analysis |
| Data contract | Accord vérifié mécaniquement, pas une simple documentation passive |
| Droit à l'oubli sur Iceberg | MoR pour la rapidité de suppression logique, compaction forcée pour l'effacement physique réel |
| Conformité comme exigence d'architecture | Le choix CoW/MoR n'est pas qu'une question de performance — c'est aussi une question réglementaire |

## Synthèse de la formation complète

```
J1 — Fondations               J2 — Opérations & MLOps          J3 — Gouvernance
─────────────────             ──────────────────────           ─────────────────
Table format ouvert            Versioning Git-like (Nessie)      Catalogue de gouvernance
Snapshots, manifests           CI/CD pour la donnée              Lineage de bout en bout
CoW vs MoR                     Feature Store (Feast)             Data contracts
Catalogue REST (Polaris)       MLOps tracking (MLflow)           Conformité réglementaire
```

La plateforme construite sur ces trois jours couvre l'ensemble du cycle de vie de la donnée dans un Lakehouse moderne : de l'ingestion brute à la mise en production d'un modèle ML, en passant par le versioning, la qualité et la conformité — avec, à chaque étape, des outils ouverts et interopérables plutôt qu'une suite propriétaire fermée.

## Pour aller plus loin

- Documentation OpenMetadata : [docs.open-metadata.org](https://docs.open-metadata.org/)
- Cadre data contracts (Open Data Contract Standard) : [bitol.io](https://bitol.io/)
- Texte de référence : loi ivoirienne n°2013-450 relative à la protection des données à caractère personnel
