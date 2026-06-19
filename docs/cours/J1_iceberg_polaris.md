# Jour 1 — Fondamentaux Apache Iceberg & Catalogue Polaris

**Formation Data Lakehouse — AZ-DATA Academy**
Contexte UEMOA/XOF — RetailCo, distributeur multi-pays (Côte d'Ivoire, Sénégal, Bénin, Togo, Burkina Faso)

---

## Objectifs pédagogiques

À l'issue de cette journée, vous serez capable de :

- Expliquer ce qu'est un table format ouvert et pourquoi il change l'architecture data
- Créer, écrire et interroger des tables Iceberg via Spark, en passant par un catalogue REST (Polaris)
- Comprendre et manipuler le cycle de vie des métadonnées Iceberg (snapshots, manifests, data files)
- Choisir entre Copy-on-Write (CoW) et Merge-on-Read (MoR) selon le profil de charge
- Diagnostiquer un problème d'intégration Spark + Iceberg + catalogue REST en lisant une stack trace

---

## Module 1 — Pourquoi un table format ouvert ?

### 1.1 Le problème que résout Iceberg

Avant les table formats modernes (Iceberg, Delta Lake, Hudi), un data lake sur S3/MinIO était une collection de fichiers Parquet dans des dossiers. Cette approche pose plusieurs problèmes structurels :

- **Pas d'atomicité** : une écriture qui échoue à mi-parcours laisse le dossier dans un état incohérent — un lecteur peut voir une partie des nouveaux fichiers et une partie des anciens.
- **Pas d'isolation** : un writer et un reader simultanés se marchent dessus ; il n'existe pas de notion de version stable à lire pendant qu'on écrit.
- **Listing coûteux** : pour savoir quels fichiers appartiennent à une table, il faut lister le système de fichiers — opération lente et chère sur S3 (`LIST` est facturé et plafonné en débit).
- **Schema drift incontrôlé** : ajouter une colonne signifie souvent réécrire toute la table, ou bricoler une gestion applicative du schéma.
- **Pas d'historique** : impossible de revenir à l'état de la table d'hier sans un système de versioning externe.

Un table format comme Iceberg résout ces problèmes en ajoutant une **couche de métadonnées transactionnelle** au-dessus des fichiers Parquet (ou ORC, Avro). Cette couche déclare explicitement : quel schéma a la table, quels fichiers la composent à un instant T, comment elle est partitionnée, et tout l'historique de ses évolutions.

### 1.2 Le triptyque Lakehouse

La promesse du Lakehouse est de combiner :

| | Data Warehouse classique | Data Lake classique | Lakehouse |
|---|---|---|---|
| Transactions ACID | ✅ | ❌ | ✅ |
| Schema enforcement | ✅ | ❌ (schema-on-read) | ✅ |
| Formats ouverts (Parquet) | ❌ | ✅ | ✅ |
| Coût stockage objet (S3) | ❌ | ✅ | ✅ |
| Time travel / versioning | Partiel | ❌ | ✅ |
| Accès multi-moteurs (Spark, Trino, Flink...) | ❌ | ✅ | ✅ |

Iceberg, en tant que **spécification ouverte**, est consommable par n'importe quel moteur qui implémente le client : Spark, Trino, Flink, Dremio, DuckDB. C'est ce découplage moteur/stockage qui est au cœur de l'architecture Lakehouse moderne, et c'est ce qu'on va exploiter toute la journée avec Spark **et** Trino sur les mêmes tables.

---

## Module 2 — Anatomie d'une table Iceberg

### 2.1 Les trois couches de métadonnées

Une table Iceberg est structurée en couches, chacune référençant la suivante :

```
table metadata (JSON)
   └── manifest list (Avro) — un par snapshot
          └── manifest files (Avro) — un ou plusieurs par manifest list
                 └── data files (Parquet) — les fichiers réels contenant les lignes
```

**Le fichier de métadonnées de table** (`v{N}.metadata.json`) est la racine. Il contient :
- le schéma actuel et l'historique des schémas (`schemas`, `current-schema-id`)
- les spécifications de partitionnement, présentes et passées
- la liste des **snapshots** et lequel est `current-snapshot-id`
- les propriétés de table (`format-version`, options d'écriture, etc.)

Chaque écriture (INSERT, UPDATE, DELETE, MERGE) crée un **nouveau snapshot**, et donc un nouveau fichier `metadata.json`. Le catalogue (Polaris, dans notre cas) garde la référence vers le `metadata.json` courant — c'est ce pointeur que le catalogue échange de façon atomique à chaque commit, ce qui donne l'atomicité globale.

**La manifest list** (un fichier Avro par snapshot) énumère les manifests qui composent ce snapshot, avec des statistiques résumées par manifest (plage de partitions couvertes, nombre de lignes ajoutées/supprimées) qui permettent au moteur de requête d'élaguer rapidement les manifests non pertinents sans les ouvrir.

**Les manifest files** énumèrent les data files individuels, avec leurs statistiques par colonne (min/max, null count) — c'est ce qui permet le **prédicat pushdown** au niveau fichier : Trino ou Spark peuvent ignorer un fichier Parquet entier sans le lire, juste en consultant ces statistiques.

### 2.2 Pourquoi cette architecture compte en pratique

Quand vous avez exécuté, dans le lab `lab_iceberg_avance`, une requête comme :

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM polaris.retailco.transactions.snapshots
```

vous interrogiez directement le `metadata.json` au travers d'une **metadata table** — une vue SQL sur la couche de métadonnées elle-même, sans toucher aux données. C'est une particularité d'Iceberg : la table de métadonnées est elle-même requêtable comme une table normale, avec le suffixe `.snapshots`, `.manifests`, `.files`, `.history`, `.partitions`, etc.

### 2.3 Le rôle du catalogue (pourquoi Polaris ?)

Le `metadata.json` doit être trouvable et son remplacement doit être **atomique** (un seul writer gagne en cas d'écriture concurrente). C'est le rôle du **catalogue** : il maintient, pour chaque table, le pointeur vers le `metadata.json` courant, et applique l'atomicité du changement de pointeur (compare-and-swap).

Plusieurs implémentations de catalogue existent : Hive Metastore, AWS Glue, Nessie, et **Polaris** (projet Apache, REST Catalog officiel de la spec Iceberg). Polaris implémente la **REST Catalog API** d'Iceberg — un protocole HTTP standardisé que n'importe quel moteur conforme peut consommer, sans dépendance à un client propriétaire.

Dans notre stack, Polaris joue ce rôle pour le catalogue `polaris.retailco.*`, pendant que Nessie (vu en détail demain) joue un rôle similaire mais avec versioning Git-like pour `nessie.retailco.*`.

---

## Module 3 — Premier contact : créer et écrire une table

### 3.1 La chaîne d'outils

Notre stack du jour :

| Composant | Rôle | Port local |
|---|---|---|
| MinIO | Stockage objet S3-compatible (équivalent S3 on-prem) | 9000 (API), 9001 (Console) |
| Polaris | Catalogue REST Iceberg | 8181 |
| Spark (PySpark, JupyterLab) | Moteur de calcul, écriture/lecture | 8888 (Jupyter), 8085 (Spark UI) |
| Trino | Moteur SQL fédéré, lecture haute performance | 8080 |

Le point clé à comprendre : **Spark et Trino ne se parlent jamais directement**. Ils passent tous les deux par Polaris pour savoir où sont les fichiers, et lisent/écrivent directement sur MinIO. Polaris est l'autorité sur le `metadata.json` courant ; MinIO est l'autorité sur les octets.

### 3.2 Le schéma RetailCo

Toutes les données du jour portent sur des transactions de vente RetailCo, multi-pays UEMOA :

```sql
CREATE TABLE IF NOT EXISTS polaris.retailco.transactions (
    transaction_id     STRING,
    store_id           STRING,
    store_name         STRING,
    store_country      STRING,
    customer_id        STRING,
    product_id         STRING,
    product_name       STRING,
    category           STRING,
    sub_category       STRING,
    unit_price_xof     BIGINT,
    quantity           INT,
    discount_pct       DOUBLE,
    total_amount_xof   BIGINT,
    currency           STRING,
    payment_method     STRING,
    channel            STRING,
    return_flag        BOOLEAN,
    return_reason      STRING,
    transaction_ts     TIMESTAMP,
    ingestion_ts       TIMESTAMP,
    _anomaly           STRING
)
USING iceberg
PARTITIONED BY (
    store_country,
    days(transaction_ts)
)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'format-version' = '2'
)
```

Points à noter :
- `PARTITIONED BY (store_country, days(transaction_ts))` utilise le **partitionnement caché** (hidden partitioning) d'Iceberg : `days(transaction_ts)` est une transformation de partition, pas une colonne physique distincte. Contrairement à Hive, vous n'avez pas besoin d'ajouter une colonne `transaction_date` séparée et de la maintenir manuellement — Iceberg dérive la partition automatiquement à l'écriture, et le moteur de requête sait l'exploiter pour l'élagage sans que vous ayez à filtrer explicitement sur une colonne technique.
- `format-version = 2` active les fonctionnalités avancées (row-level deletes, equality deletes) qu'on explore au Module 5.
- Le montant est en `BIGINT`, pas `DOUBLE` : le XOF n'a pas de sous-unité (1 XOF = 1 XOF, pas de centimes), donc les montants sont stockés en entiers, ce qui évite tout problème d'arrondi flottant sur des agrégations financières.

### 3.3 Le piège classique : schéma cible vs données source

Un des bugs les plus fréquents en ingestion Iceberg (que vous avez rencontré en TP) est le **mismatch de colonnes** entre un `INSERT INTO ... SELECT` et le schéma cible. Iceberg est strict : le nombre de colonnes du `SELECT` doit correspondre exactement à celui de la table, dans le même ordre. Si votre `SELECT` ne produit que 9 colonnes pour une table qui en attend 21, Spark renvoie :

```
[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS]
```

La correction consiste à lister explicitement les 21 colonnes dans le `SELECT`, en complétant par `NULL` les colonnes non pertinentes au cas d'usage :

```sql
INSERT INTO polaris.retailco.transactions_mor
SELECT
    transaction_id, store_id, store_name, store_country, customer_id,
    product_id, product_name, category, sub_category,
    CAST(unit_price_xof * 1.01 AS BIGINT),
    quantity, discount_pct,
    CAST(total_amount_xof * 1.01 AS BIGINT),
    currency, payment_method, channel,
    return_flag, return_reason, transaction_ts, ingestion_ts, _anomaly
FROM polaris.retailco.transactions LIMIT 3
```

Notez aussi le `CAST(... AS BIGINT)` : une multiplication par un flottant Python (`1.01`) promeut le résultat en type décimal, qui doit être recasté explicitement vers le type `BIGINT` attendu par la colonne — sinon Spark lève une erreur de type au lieu d'une conversion implicite silencieuse.

---

## Module 4 — Snapshots, manifests, time travel

### 4.1 Le modèle de snapshot

Chaque opération d'écriture (`INSERT`, `DELETE`, `MERGE`, `UPDATE`) crée un nouveau **snapshot** : un point dans le temps immuable, identifié par un `snapshot_id` numérique, qui référence un ensemble figé de manifests et donc de data files.

```sql
SELECT snapshot_id, committed_at, operation, summary
FROM polaris.retailco.transactions.snapshots
ORDER BY committed_at DESC
```

La colonne `operation` indique le type de commit (`append`, `overwrite`, `delete`, `replace`), et `summary` contient des métriques utiles : `added-data-files`, `added-records`, `total-records`.

### 4.2 Time travel

Comme chaque snapshot est conservé (jusqu'à expiration explicite), vous pouvez interroger l'état de la table à un instant T :

```sql
-- Par identifiant de snapshot
SELECT * FROM polaris.retailco.transactions VERSION AS OF 8466212344228261521;

-- Par horodatage
SELECT * FROM polaris.retailco.transactions TIMESTAMP AS OF '2026-06-18 10:00:00';
```

C'est la base du **rollback** sans sauvegarde externe : si une écriture corrompt la table, il suffit de réassigner le pointeur du catalogue vers un snapshot antérieur — opération que vous approfondirez demain avec Nessie, qui généralise ce mécanisme à l'échelle de branches entières.

### 4.3 Compaction et fichiers orphelins

Le revers du modèle de snapshot : chaque commit ajoute des fichiers et n'en supprime physiquement aucun (les anciens `data files` restent sur MinIO tant qu'un snapshot les référence). Sur une table avec beaucoup de petites écritures, ceci produit une accumulation de petits fichiers Parquet, néfaste pour la performance de lecture.

Iceberg fournit une procédure standard de compaction :

```sql
CALL polaris.system.rewrite_data_files(
    table => 'retailco.transactions',
    options => map('target-file-size-bytes', '134217728')
)
```

Et pour purger les anciens snapshots devenus inutiles (au-delà d'une fenêtre de rétention) :

```sql
CALL polaris.system.expire_snapshots(
    table => 'retailco.transactions',
    older_than => TIMESTAMP '2026-06-01 00:00:00'
)
```

Sans cette maintenance périodique, le volume de stockage MinIO croît indéfiniment même si la table « logique » ne grossit pas.

---

## Module 5 — Format-versions et stratégies de delete : CoW vs MoR

C'est le sujet du lab `lab_iceberg_format_versions`, où vous avez comparé trois variantes de la même table.

### 5.1 Le problème des suppressions sur fichiers immuables

Les fichiers Parquet sont **immuables** — on ne peut pas modifier une ligne en place. Pour appliquer un `DELETE` ou un `UPDATE`, Iceberg a deux stratégies possibles :

**Copy-on-Write (CoW)** : à chaque suppression, Iceberg réécrit entièrement le ou les fichiers Parquet affectés, en excluant les lignes supprimées. Le nouveau fichier remplace l'ancien dans le nouveau snapshot.

- ✅ Lecture rapide (les fichiers de données ne contiennent que les lignes valides, pas de filtrage à la lecture)
- ❌ Écriture coûteuse (réécrire potentiellement un gros fichier pour supprimer une seule ligne)

**Merge-on-Read (MoR)** : à chaque suppression, Iceberg écrit un petit fichier de **delete** (positional delete ou equality delete) qui référence les lignes à exclure, sans toucher au fichier de données original. Le rapprochement (merge) se fait à la lecture.

- ✅ Écriture rapide (un petit fichier de delete, pas de réécriture)
- ❌ Lecture plus lente à mesure que les delete files s'accumulent (le moteur doit les fusionner à chaque scan), nécessite une compaction périodique

### 5.2 Les deux types de delete files en MoR

- **Positional deletes** (`content = 1` dans la metadata table `.files`) : référencent une ligne par `(chemin de fichier, numéro de ligne)`. Rapides à écrire et à appliquer.
- **Equality deletes** (`content = 2`) : référencent les lignes à supprimer par valeur de colonne (ex: `WHERE customer_id = 'cust-001'`), utiles pour les suppressions basées sur une clé sans connaître la position physique.

```sql
SELECT content, count(*) AS n, sum(file_size_in_bytes) AS total_bytes
FROM polaris.versions_demo.transactions_v2.files
GROUP BY content
```

`content = 0` désigne les data files normaux ; cette requête vous a permis d'observer concrètement, dans le lab, l'accumulation de delete files après une série de suppressions unitaires sur une table MoR.

### 5.3 Format-version 3 et les Deletion Vectors

Le `format-version = 3` (le plus récent de la spec Iceberg) introduit les **Deletion Vectors** : un format de delete plus compact que les positional deletes classiques, basé sur des bitmaps binaires (format Puffin), offrant jusqu'à 10× la compacité et une meilleure performance de lecture pour les tables MoR à forte volumétrie de suppressions.

### 5.4 Quand choisir quoi

| Profil de charge | Stratégie recommandée |
|---|---|
| Beaucoup d'écritures, peu de lectures, tolérant à la latence de lecture | MoR + compaction régulière |
| Lecture intensive, peu de mises à jour | CoW |
| Conformité RGPD / droit à l'oubli (suppressions ciblées fréquentes) | MoR avec Deletion Vectors (v3) |
| Pipelines batch avec gros volumes de delete en une fois | CoW (le coût de réécriture est amorti sur tout le batch) |

---

## Module 6 — Le catalogue Polaris en détail

### 6.1 Modèle RBAC de Polaris

Polaris implémente un modèle de contrôle d'accès à plusieurs niveaux :

```
Principal (ex: root)
   └── Principal Role (ex: service_admin)
          └── Catalog Role (ex: retailco_admin_role)
                 └── Privilège (ex: CATALOG_MANAGE_CONTENT) sur un Catalog
```

Un point qui a posé des difficultés en TP infra : le principal `root`, même avec le rôle `service_admin` par défaut au bootstrap, **ne possède pas automatiquement** le privilège `CATALOG_MANAGE_CONTENT` nécessaire à certaines opérations avancées (notamment la délégation de credentials S3). Ce privilège doit être accordé explicitement via un catalog role dédié — c'est ce que fait le script `setup-polaris.sh` du repo, en suivant le pattern officiel du quickstart Polaris.

### 6.2 Interroger Polaris directement en REST

Au-delà de Spark, vous pouvez dialoguer directement avec l'API REST de Polaris — utile pour scripter de l'administration ou déboguer :

```python
import requests

token_resp = requests.post(
    "http://polaris:8181/api/catalog/v1/oauth/tokens",
    data={
        "grant_type": "client_credentials",
        "client_id": "root",
        "client_secret": "s3cr3t",
        "scope": "PRINCIPAL_ROLE:ALL",
    },
)
token = token_resp.json()["access_token"]

# Lister les namespaces du catalog retailco
namespaces = requests.get(
    "http://polaris:8181/api/catalog/v1/retailco/namespaces",
    headers={"Authorization": f"Bearer {token}"},
).json()
```

Point d'attention important, identifié lors de la mise en place de l'infrastructure : **tous les chemins REST Iceberg/Polaris exigent le préfixe du catalogue** (`retailco`) entre `/v1/` et `/namespaces` — l'omettre produit une erreur 404 trompeuse qui ressemble à un problème d'authentification.

### 6.3 storageType et credential vending

Lorsque Polaris est configuré pour MinIO (`storageType: S3`), deux endpoints sont déclarés dans le catalogue :
- `endpoint` : l'adresse accessible **depuis l'extérieur de Docker** (ex: `http://localhost:9000`, utile pour un client tournant sur la machine hôte)
- `endpointInternal` : l'adresse accessible **depuis l'intérieur du réseau Docker** (ex: `http://minio:9000`, utilisée par Polaris et par Spark/Trino qui tournent eux-mêmes en containers)

Avec l'option `stsUnavailable: true` (utilisée dans ce repo, car MinIO ne supporte pas pleinement le mécanisme STS d'AWS), Polaris ne tente pas de déléguer des credentials temporaires dynamiquement et utilise directement les credentials statiques configurés — un choix pragmatique qui simplifie le déploiement en environnement de formation, au prix d'une isolation de sécurité moindre qu'en production avec un vrai provider STS (AWS IAM, par exemple).

---

## Synthèse J1

| Concept | À retenir |
|---|---|
| Table format ouvert | Couche de métadonnées transactionnelle au-dessus de fichiers Parquet |
| Snapshot | Point dans le temps immuable créé à chaque commit ; base du time travel |
| Manifest / Manifest list | Index permettant l'élagage de fichiers sans les ouvrir |
| Hidden partitioning | Partition dérivée d'une transformation (`days(...)`), pas une colonne physique |
| Catalogue REST (Polaris) | Autorité sur le pointeur `metadata.json` courant ; garantit l'atomicité des commits |
| CoW vs MoR | Arbitrage écriture rapide / lecture rapide selon le profil de charge |
| Deletion Vectors (v3) | Format de delete compact, successeur des positional deletes classiques |

## Pour aller plus loin

- Spécification Iceberg : [iceberg.apache.org/spec](https://iceberg.apache.org/spec/)
- Documentation Polaris : [polaris.apache.org](https://polaris.apache.org/)
- RFC sur les Deletion Vectors : *Iceberg V3 Spec — Deletion Vectors*
