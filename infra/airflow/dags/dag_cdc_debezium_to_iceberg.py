"""
dag_cdc_debezium_to_iceberg.py
================================
Démo CDC : consomme les événements Debezium depuis Kafka
et les applique sur la table Iceberg Bronze en mode UPSERT (MoR V2).

Pattern : Debezium → Kafka → Airflow Sensor → Spark MERGE INTO Iceberg

Utilisé pour la démo formateur J2 (profil cdc)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator

DEFAULT_ARGS = {
    'owner':            'data-engineering',
    'depends_on_past':  False,
    'retries':          1,
    'retry_delay':      timedelta(minutes=2),
    'start_date':       datetime(2026, 1, 1),
}

KAFKA_BOOTSTRAP = 'kafka:29092'
TOPIC           = 'retailco.retailco.transactions'
NESSIE_URI      = 'http://nessie:19120/api/v2'
MINIO_ENDPOINT  = 'http://minio:9000'


class KafkaMessageSensor(BaseSensorOperator):
    """Vérifie qu'il y a des messages non consommés sur le topic Debezium."""

    def __init__(self, topic: str, bootstrap_servers: str, **kwargs):
        super().__init__(**kwargs)
        self.topic             = topic
        self.bootstrap_servers = bootstrap_servers

    def poke(self, context):
        try:
            from kafka import KafkaConsumer, TopicPartition
            consumer = KafkaConsumer(
                bootstrap_servers = self.bootstrap_servers,
                group_id          = 'airflow-cdc-sensor',
                auto_offset_reset = 'latest',
            )
            partitions = consumer.partitions_for_topic(self.topic)
            if not partitions:
                self.log.info(f'Topic {self.topic} vide ou inexistant')
                return False
            tp = [TopicPartition(self.topic, p) for p in partitions]
            end_offsets = consumer.end_offsets(tp)
            has_messages = any(v > 0 for v in end_offsets.values())
            consumer.close()
            return has_messages
        except Exception as e:
            self.log.warning(f'KafkaMessageSensor: {e}')
            return False


def consume_and_apply(**ctx):
    """
    Consomme les événements CDC depuis Kafka et applique via MERGE INTO Iceberg.

    Format Debezium (unwrapped via ExtractNewRecordState) :
    {
      "transaction_id": "txn-001",
      "total_amount": 599.0,
      "__deleted": "false",   ← présent si delete.handling.mode=rewrite
      ...
    }
    """
    try:
        from kafka import KafkaConsumer
        import json
    except ImportError:
        print('⚠️  kafka-python non disponible — simulation CDC')
        return

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP,
        group_id          = 'airflow-cdc-apply',
        auto_offset_reset = 'earliest',
        enable_auto_commit= False,
        value_deserializer= lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms = 10000,
    )

    upserts, deletes = [], []
    for msg in consumer:
        record = msg.value
        if record.get('__deleted') == 'true':
            deletes.append(record.get('transaction_id'))
        else:
            upserts.append(record)
        if len(upserts) + len(deletes) >= 500:
            break

    consumer.commit()
    consumer.close()

    if not upserts and not deletes:
        print('Aucun événement CDC à appliquer')
        return

    # Appliquer via Spark MERGE INTO
    from pyspark.sql import SparkSession
    import os

    spark = SparkSession.builder.appName('CDC-Apply').getOrCreate()

    if upserts:
        import pandas as pd
        df_updates = spark.createDataFrame(pd.DataFrame(upserts))
        df_updates.createOrReplaceTempView('cdc_updates')
        spark.sql("""
            MERGE INTO nessie.retailco.transactions_bronze t
            USING cdc_updates u ON t.transaction_id = u.transaction_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f'✅ {len(upserts)} UPSERT appliqués via MERGE INTO')

    if deletes:
        ids = "', '".join(deletes)
        spark.sql(f"""
            DELETE FROM nessie.retailco.transactions_bronze
            WHERE transaction_id IN ('{ids}')
        """)
        print(f'✅ {len(deletes)} DELETE appliqués')

    ctx['ti'].xcom_push(key='cdc_events', value=len(upserts) + len(deletes))


def log_cdc_stats(**ctx):
    events = ctx['ti'].xcom_pull(task_ids='consume_and_apply', key='cdc_events') or 0
    print(f'📊 CDC stats — {ctx["ds"]} : {events} événements traités')


with DAG(
    dag_id       = 'cdc_debezium_to_iceberg',
    description  = 'CDC Debezium → Kafka → Iceberg MERGE INTO (démo formateur J2)',
    default_args = DEFAULT_ARGS,
    schedule     = '*/15 * * * *',   # toutes les 15 min
    catchup      = False,
    tags         = ['cdc', 'debezium', 'kafka', 'iceberg', 'formation'],
) as dag:

    start = EmptyOperator(task_id='start')

    sense = KafkaMessageSensor(
        task_id            = 'wait_for_cdc_events',
        topic              = TOPIC,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        poke_interval      = 30,
        timeout            = 300,
        mode               = 'poke',
    )

    apply = PythonOperator(
        task_id         = 'consume_and_apply',
        python_callable = consume_and_apply,
    )

    stats = PythonOperator(
        task_id         = 'log_cdc_stats',
        python_callable = log_cdc_stats,
    )

    start >> sense >> apply >> stats
