"""
healthtech_lakehouse_dag.py — Orquestração de Pipeline Data Lakehouse no Airflow 3
==================================================================================
Responsabilidade: Orquestração diária automatizada da esteira de dados hospitalares
sob a Arquitetura Medalhão (Bronze -> Silver -> Gold) com PySpark e Delta Lake.

Padrões de Produção:
  - Lazy Imports para isolamento de dependências e otimização do Scheduler.
  - Idempotência em todas as tarefas via Delta Lake Overwrite particionado.
  - Tratamento de exceções e políticas de retry para resiliência operacional.
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Inclusão do diretório do projeto no sys.path para importação do pacote src
PROJECT_DIR = "/home/ericl/projetos/healthtech-lakehouse"
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# Definição dos caminhos de armazenamento do Data Lakehouse
PATH_BRONZE = os.path.join(PROJECT_DIR, "storage", "bronze")
PATH_SILVER = os.path.join(PROJECT_DIR, "storage", "silver")
PATH_GOLD   = os.path.join(PROJECT_DIR, "storage", "gold")


def get_spark_session(app_name: str = "AirflowHealthTechLakehouse"):
    """
    Inicializa a SparkSession com extensões Delta Lake sob demanda (Lazy Import).
    Evita inicialização prematura da JVM durante os ciclos de parsing do Scheduler.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]")

    return configure_spark_with_delta_pip(builder).getOrCreate()


# ===========================================================================
# CALLABLES DAS TAREFAS (Executadas pelos Workers do Airflow)
# ===========================================================================

def run_ingest_bronze(**context):
    """Executa a ingestão de prontuários com Schema Enforcement na Bronze."""
    from src.bronze import ingest_bronze

    spark = get_spark_session("Airflow_HealthTech_Bronze")
    spark.sparkContext.setLogLevel("WARN")

    total = ingest_bronze(spark, PATH_BRONZE, num_records=500)
    print(f"[Airflow Task] Ingestão Bronze concluída. Total de registros: {total}")
    return total


def run_transform_silver(**context):
    """Executa a limpeza, deduplicação e cálculo de métricas clínicas na Silver."""
    from src.silver import transform_silver

    spark = get_spark_session("Airflow_HealthTech_Silver")
    spark.sparkContext.setLogLevel("WARN")

    total = transform_silver(spark, PATH_BRONZE, PATH_SILVER)
    print(f"[Airflow Task] Transformação Silver concluída. Registros curados: {total}")
    return total


def run_load_gold(**context):
    """Executa a modelagem dimensional analítica (Fato e Dimensões) na Gold."""
    from src.gold import load_gold

    spark = get_spark_session("Airflow_HealthTech_Gold")
    spark.sparkContext.setLogLevel("WARN")

    result = load_gold(spark, PATH_SILVER, PATH_GOLD)
    print(f"[Airflow Task] Carga Gold concluída. Tabela Fato: {result['fact_hospital_occupancy']} registros")
    return result


# ===========================================================================
# DEFINIÇÃO DA DAG
# ===========================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="healthtech_lakehouse_pyspark_pipeline",
    default_args=default_args,
    description="Pipeline Medalhão Hospitalar (Bronze -> Silver -> Gold) com PySpark e Delta Lake",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["healthtech", "pyspark", "delta-lake", "medallion", "hospital-analytics"],
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id="ingest_bronze_task",
        python_callable=run_ingest_bronze,
    )

    transform_silver_task = PythonOperator(
        task_id="transform_silver_task",
        python_callable=run_transform_silver,
    )

    load_gold_task = PythonOperator(
        task_id="load_gold_task",
        python_callable=run_load_gold,
    )

    # Encadeamento estrito da esteira de dados
    ingest_bronze_task >> transform_silver_task >> load_gold_task
