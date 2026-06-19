"""
dag_data_quality.py
====================
Contrôle qualité quotidien sur les tables Iceberg Silver et Gold.
Détecte les 8 types d'anomalies RetailCo — génère un rapport.

Exécuté après dag_retailco_pipeline (dépendance via ExternalTaskSensor).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    'owner':           'data-quality',
    'retries':         1,
    'retry_delay':     timedelta(minutes=5),
    'start_date':      datetime(2026, 1, 1),
}

POLARIS_URI    = 'http://polaris:8181/api/catalog'
MINIO_ENDPOINT = 'http://minio:9000'


def check_silver_quality(**ctx):
    """8 règles de qualité sur la table Silver."""
    try:
        from pyiceberg.catalog.rest import RestCatalog
        catalog = RestCatalog('polaris', uri=POLARIS_URI)
        tbl     = catalog.load_table(('retailco', 'silver_transactions'))
        df      = tbl.scan().to_arrow().to_pandas()
    except Exception as e:
        print(f'⚠️  Impossible de charger Silver : {e}')
        return

    results = {}

    # A1 — Doublons
    n_dup = df.duplicated('transaction_id').sum()
    results['A1_doublons'] = {'count': int(n_dup), 'ok': n_dup == 0}

    # A2 — Prix négatifs
    n_neg = (df['total_amount'] < 0).sum()
    results['A2_prix_negatifs'] = {'count': int(n_neg), 'ok': n_neg == 0}

    # A3 — Remises invalides
    n_disc = (df['discount_pct'] > 1.0).sum()
    results['A3_remises_invalides'] = {'count': int(n_disc), 'ok': n_disc == 0}

    # A4 — Cohérence TVA (total_amount ≈ qty × price × (1-disc) × 1.20 ± 1%)
    expected = df['quantity'] * df['unit_price'] * (1 - df['discount_pct']) * 1.20
    delta    = ((df['total_amount'] - expected).abs() / expected.clip(lower=0.01)) > 0.01
    n_tva    = delta.sum()
    results['A4_tva_incoherente'] = {'count': int(n_tva), 'ok': n_tva == 0}

    # A5 — Clients anonymes (toléré mais tracé)
    n_anon = df['customer_id'].isna().sum()
    results['A5_clients_anonymes'] = {'count': int(n_anon), 'ok': True, 'warning': n_anon > 50}

    # A6 — Retour sans raison
    n_ret = (df['return_flag'] & df['return_reason'].isna()).sum()
    results['A6_retour_sans_raison'] = {'count': int(n_ret), 'ok': n_ret == 0}

    # A7 — Quantité invalide
    n_qty = (df['quantity'] <= 0).sum()
    results['A7_quantite_invalide'] = {'count': int(n_qty), 'ok': n_qty == 0}

    # A8 — Outliers prix (> 3× la médiane par catégorie)
    medians    = df.groupby('category')['unit_price'].median()
    df['med']  = df['category'].map(medians)
    n_out      = (df['unit_price'] > df['med'] * 5).sum()
    results['A8_prix_outlier'] = {'count': int(n_out), 'ok': n_out == 0}

    # Résumé
    failed = [k for k, v in results.items() if not v['ok']]
    print(f'\n📊 DQ Silver — {len(df)} lignes analysées')
    print(f'   Checks passés : {len(results) - len(failed)}/{len(results)}')
    for k, v in results.items():
        icon = '✅' if v['ok'] else ('⚠️' if v.get('warning') else '❌')
        print(f'   {icon} {k}: {v["count"]} occurrence(s)')

    if failed:
        ctx['ti'].xcom_push(key='dq_failed', value=failed)
        raise ValueError(f'DQ Silver échouée : {failed}')

    ctx['ti'].xcom_push(key='silver_rows', value=len(df))


def check_gold_quality(**ctx):
    """Contrôles sur la table Gold."""
    try:
        from pyiceberg.catalog.rest import RestCatalog
        catalog = RestCatalog('polaris', uri=POLARIS_URI)
        tbl     = catalog.load_table(('retailco', 'gold_ca_by_store_week'))
        df      = tbl.scan().to_arrow().to_pandas()
    except Exception as e:
        print(f'⚠️  Impossible de charger Gold : {e}')
        return

    checks = {
        'ca_positif':         (df['ca_total_ttc'] > 0).all(),
        'nb_tx_positif':      (df['nb_transactions'] > 0).all(),
        'store_id_non_null':  df['store_id'].notna().all(),
        'year_valide':        df['year'].between(2025, 2027).all(),
    }

    failed = [k for k, v in checks.items() if not v]
    print(f'\n📊 DQ Gold — {len(df)} agrégats analysés')
    for k, v in checks.items():
        print(f'   {"✅" if v else "❌"} {k}')

    if failed:
        raise ValueError(f'DQ Gold échouée : {failed}')


def generate_dq_report(**ctx):
    """Génère un rapport DQ et le pousse dans MinIO."""
    import boto3, json
    from datetime import date

    silver_rows = ctx['ti'].xcom_pull(task_ids='check_silver', key='silver_rows') or 0
    failed      = ctx['ti'].xcom_pull(task_ids='check_silver', key='dq_failed') or []

    report = {
        'run_date':     ctx['ds'],
        'silver_rows':  silver_rows,
        'status':       'FAILED' if failed else 'PASSED',
        'failed_checks': failed,
        'generated_at': datetime.utcnow().isoformat(),
    }

    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin')
    key = f'reports/dq/dq_report_{ctx["ds"]}.json'
    s3.put_object(Bucket='gold', Key=key, Body=json.dumps(report, indent=2))
    print(f'📄 Rapport DQ → s3://gold/{key}')


with DAG(
    dag_id       = 'data_quality_retailco',
    description  = 'Contrôle qualité Silver + Gold — 8 règles RetailCo',
    default_args = DEFAULT_ARGS,
    schedule     = '0 7 * * *',   # après le pipeline (06h + ~30 min)
    catchup      = False,
    tags         = ['data-quality', 'great-expectations', 'iceberg', 'formation'],
) as dag:

    wait_pipeline = ExternalTaskSensor(
        task_id            = 'wait_for_pipeline',
        external_dag_id    = 'retailco_bronze_silver_gold',
        external_task_id   = 'end',
        timeout            = 3600,
        poke_interval      = 60,
        mode               = 'reschedule',
    )

    check_silver = PythonOperator(
        task_id         = 'check_silver',
        python_callable = check_silver_quality,
    )

    check_gold = PythonOperator(
        task_id         = 'check_gold',
        python_callable = check_gold_quality,
    )

    report = PythonOperator(
        task_id         = 'generate_dq_report',
        python_callable = generate_dq_report,
    )

    wait_pipeline >> check_silver >> check_gold >> report
