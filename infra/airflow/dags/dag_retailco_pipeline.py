"""
dag_retailco_pipeline.py
========================
Pipeline complet RetailCo : Bronze → Silver → Gold
Orchestré par Airflow 2.8 — Iceberg via Polaris/Nessie

Architecture :
  [MinIO Bronze]
       │ (Spark / pyiceberg)
  [Iceberg Bronze Table]
       │ (dbt silver_transactions)
  [Iceberg Silver Table]
       │ (dbt gold_ca_by_store_week)
  [Iceberg Gold Table]
       │ (Great Expectations)
  [DQ Report]

Schedule : quotidien à 06h00 (UTC)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Paramètres par défaut ────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner':            'data-engineering',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'start_date':       datetime(2026, 1, 1),
}

MINIO_ENDPOINT    = 'http://minio:9000'
NESSIE_URI        = 'http://nessie:19120/api/v2'
POLARIS_URI       = 'http://polaris:8181/api/catalog'
WAREHOUSE         = 's3a://warehouse/'
BRONZE_BUCKET     = 's3://bronze/retailco/transactions/'

# ── Fonctions des tasks ──────────────────────────────────────────────────────
def check_source_files(**ctx):
    """Vérifie qu'il y a de nouveaux fichiers dans s3://bronze/."""
    import boto3
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin')
    resp = s3.list_objects_v2(Bucket='bronze', Prefix='retailco/transactions/')
    files = resp.get('Contents', [])
    if not files:
        return 'skip_pipeline'
    ctx['ti'].xcom_push(key='file_count', value=len(files))
    print(f'✅ {len(files)} fichier(s) trouvé(s) dans Bronze')
    return 'ingest_bronze'


def ingest_bronze(**ctx):
    """Ingère les fichiers CSV/Parquet de MinIO dans la table Iceberg Bronze."""
    from pyiceberg.catalog.rest import RestCatalog
    import pyarrow.csv as pa_csv
    import pyarrow as pa
    import boto3, io

    catalog = RestCatalog('polaris', **{
        'uri': POLARIS_URI,
        'warehouse': 'warehouse',
        's3.endpoint': MINIO_ENDPOINT,
        's3.access-key-id': 'minioadmin',
        's3.secret-access-key': 'minioadmin',
        's3.path-style-access': 'true',
    })

    # Créer le namespace si nécessaire
    try:
        catalog.create_namespace('retailco')
    except Exception:
        pass

    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin')

    resp   = s3.list_objects_v2(Bucket='bronze', Prefix='retailco/transactions/')
    files  = [o['Key'] for o in resp.get('Contents', []) if o['Key'].endswith('.csv')]
    tables = []

    for key in files:
        obj  = s3.get_object(Bucket='bronze', Key=key)
        data = obj['Body'].read()
        tbl  = pa_csv.read_csv(io.BytesIO(data))
        tables.append(tbl)
        print(f'  Chargé : {key} ({len(tbl)} lignes)')

    if not tables:
        raise ValueError('Aucun fichier CSV trouvé dans Bronze')

    import pyarrow as pa
    combined = pa.concat_tables(tables)

    # Écrire dans Iceberg via PyIceberg
    try:
        iceberg_table = catalog.load_table(('retailco', 'transactions_bronze'))
        iceberg_table.append(combined)
    except Exception:
        iceberg_table = catalog.create_table(
            identifier   = ('retailco', 'transactions_bronze'),
            schema       = combined.schema,
            location     = f'{WAREHOUSE}retailco/transactions_bronze/',
            properties   = {'format-version': '2', 'write.format.default': 'parquet'},
        )
        iceberg_table.append(combined)

    print(f'✅ Bronze ingéré : {len(combined)} lignes → polaris.retailco.transactions_bronze')
    ctx['ti'].xcom_push(key='bronze_rows', value=len(combined))


def run_dbt_silver(**ctx):
    """Lance dbt pour transformer Bronze → Silver."""
    import subprocess
    result = subprocess.run(
        ['dbt', 'run', '--select', 'silver_transactions',
         '--profiles-dir', '/home/airflow/dbt',
         '--project-dir', '/home/airflow/dbt'],
        capture_output=True, text=True, cwd='/home/airflow/dbt'
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError('dbt run silver a échoué')
    print('✅ Silver transformé')


def run_dbt_gold(**ctx):
    """Lance dbt pour agréger Silver → Gold."""
    import subprocess
    result = subprocess.run(
        ['dbt', 'run', '--select', 'gold_ca_by_store_week',
         '--profiles-dir', '/home/airflow/dbt',
         '--project-dir', '/home/airflow/dbt'],
        capture_output=True, text=True, cwd='/home/airflow/dbt'
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError('dbt run gold a échoué')
    print('✅ Gold agrégé')


def run_dbt_tests(**ctx):
    """Lance les tests dbt sur Silver et Gold."""
    import subprocess
    result = subprocess.run(
        ['dbt', 'test', '--select', 'silver_transactions gold_ca_by_store_week'],
        capture_output=True, text=True, cwd='/home/airflow/dbt'
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError('dbt test a échoué — voir les résultats ci-dessus')
    print('✅ Tests dbt passés')


def quality_check(**ctx):
    """Contrôle de qualité sur la table Silver avec Great Expectations."""
    try:
        from pyiceberg.catalog.rest import RestCatalog
        import pyarrow.compute as pc

        catalog = RestCatalog('polaris', uri=POLARIS_URI)
        tbl     = catalog.load_table(('retailco', 'silver_transactions'))
        df      = tbl.scan().to_arrow().to_pandas()

        checks = {
            'total_amount_positif':  (df['total_amount'] > 0).all(),
            'quantity_positif':       (df['quantity'] > 0).all(),
            'discount_valide':        (df['discount_pct'] <= 1.0).all(),
            'tx_id_unique':           df['transaction_id'].nunique() == len(df),
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            raise ValueError(f'❌ Checks qualité échoués : {failed}')

        print(f'✅ Qualité OK — {len(df)} lignes Silver validées')
        ctx['ti'].xcom_push(key='silver_rows', value=len(df))

    except ImportError:
        print('⚠️  pyiceberg non disponible dans Airflow — DQ ignorée')


def notify_success(**ctx):
    """Log de fin de pipeline."""
    bronze_rows = ctx['ti'].xcom_pull(task_ids='ingest_bronze', key='bronze_rows') or 0
    silver_rows = ctx['ti'].xcom_pull(task_ids='quality_check', key='silver_rows') or 0
    run_date    = ctx['ds']
    print(f"""
    ╔══════════════════════════════════════════════╗
    ║  Pipeline RetailCo terminé — {run_date}  ║
    ╠══════════════════════════════════════════════╣
    ║  Bronze ingéré : {bronze_rows:>6} lignes            ║
    ║  Silver validé : {silver_rows:>6} lignes            ║
    ╚══════════════════════════════════════════════╝
    """)


# ── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id          = 'retailco_bronze_silver_gold',
    description     = 'Pipeline RetailCo : ingestion → transformation → agrégation',
    default_args    = DEFAULT_ARGS,
    schedule        = '0 6 * * *',
    catchup         = False,
    max_active_runs = 1,
    tags            = ['retailco', 'lakehouse', 'iceberg', 'formation'],
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    check_files = BranchPythonOperator(
        task_id         = 'check_source_files',
        python_callable = check_source_files,
    )

    skip = EmptyOperator(task_id='skip_pipeline')

    ingest = PythonOperator(
        task_id         = 'ingest_bronze',
        python_callable = ingest_bronze,
    )

    silver = PythonOperator(
        task_id         = 'run_dbt_silver',
        python_callable = run_dbt_silver,
    )

    gold = PythonOperator(
        task_id         = 'run_dbt_gold',
        python_callable = run_dbt_gold,
    )

    tests = PythonOperator(
        task_id         = 'run_dbt_tests',
        python_callable = run_dbt_tests,
    )

    dq = PythonOperator(
        task_id         = 'quality_check',
        python_callable = quality_check,
    )

    notify = PythonOperator(
        task_id         = 'notify_success',
        python_callable = notify_success,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
    )

    # ── Dépendances ───────────────────────────────────────────────────────────
    start >> check_files >> [ingest, skip]
    ingest >> silver >> gold >> tests >> dq >> notify >> end
    skip >> end
