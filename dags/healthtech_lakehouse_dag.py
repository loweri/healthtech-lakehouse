"""
healthtech_lakehouse_dag.py — Orquestração de Pipeline Data Lakehouse no Airflow 3
==================================================================================
Responsabilidade: Orquestração diária automatizada da esteira de dados hospitalares
sob a Arquitetura Medalhão (Bronze -> Silver -> Gold) com PySpark e Delta Lake.

Padrões de Produção:
  - Lazy Imports para isolamento de dependências e otimização do Scheduler.
  - PROJECT_DIR resolvido dinamicamente via pathlib (portável entre máquinas).
  - JAVA_HOME injetado em os.environ dentro dos callables (Airflow 3 workers não
    herdam variáveis de ambiente do shell de quem iniciou o `airflow standalone`).
  - Idempotência em todas as tarefas via Delta Lake Overwrite particionado.
  - Tratamento de exceções e políticas de retry para resiliência operacional.

Correções aplicadas (PR de melhoria):
  - PROJECT_DIR era hardcoded para /home/ericl/projetos/healthtech-lakehouse,
    causando ModuleNotFoundError: No module named 'src' em qualquer outra máquina.
  - JAVA_HOME não era injetado, causando PySparkRuntimeError: JAVA_GATEWAY_EXITED
    nos workers do Airflow 3 (LocalExecutor).
"""

from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Resolução do diretório raiz do projeto — estratégia em cascata
# ---------------------------------------------------------------------------
# O Airflow 3 (LocalDagBundle) pode copiar/servir o DAG de um diretório de cache,
# fazendo com que Path(__file__) não aponte para o repositório original.
# Usamos 3 estratégias em ordem de prioridade:
#
#   1. Variável de ambiente HEALTHTECH_PROJECT_DIR (mais confiável em produção).
#      Configure no ambiente antes de iniciar o Airflow:
#        export HEALTHTECH_PROJECT_DIR=/home/pacod/github/healthtech-lakehouse
#
#   2. Resolução via __file__ (funciona quando o dags_folder aponta direto ao repo).
#      Este arquivo: <PROJECT_DIR>/dags/healthtech_lakehouse_dag.py
#      .parent.parent => <PROJECT_DIR>
#
#   3. Fallback para o path absoluto conhecido nesta máquina.

def _resolve_project_dir() -> str:
    """Resolve o PROJECT_DIR usando múltiplas estratégias de fallback."""
    # Estratégia 1: variável de ambiente explícita
    env_dir = os.environ.get("HEALTHTECH_PROJECT_DIR", "")
    if env_dir and Path(env_dir, "src").is_dir():
        return env_dir

    # Estratégia 2: path relativo a este arquivo (funciona quando dags_folder = repo/dags)
    candidate = str(Path(__file__).resolve().parent.parent)
    if Path(candidate, "src").is_dir():
        return candidate

    # Estratégia 3: fallback absoluto para esta máquina
    fallback = str(Path.home() / "github" / "healthtech-lakehouse")
    if Path(fallback, "src").is_dir():
        return fallback

    raise RuntimeError(
        f"Não foi possível localizar o diretório raiz do projeto HealthTech.\n"
        f"Defina a variável de ambiente HEALTHTECH_PROJECT_DIR apontando para a "
        f"raiz do repositório (diretório que contém a pasta 'src/')."
    )


PROJECT_DIR = _resolve_project_dir()

# Garante que 'src' seja importável no contexto de parsing do DAG
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

# Definição dos caminhos de armazenamento do Data Lakehouse
PATH_BRONZE = os.path.join(PROJECT_DIR, "storage", "bronze")
PATH_SILVER = os.path.join(PROJECT_DIR, "storage", "silver")
PATH_GOLD   = os.path.join(PROJECT_DIR, "storage", "gold")

# ---------------------------------------------------------------------------
# Detecção automática do JAVA_HOME (busca JDK instalado em ~/.jdk17 ou sistema)
# ---------------------------------------------------------------------------
def _resolve_java_home() -> str:
    """
    Retorna o caminho do JAVA_HOME, priorizando:
    1. Variável de ambiente JAVA_HOME já definida no ambiente do processo.
    2. JDK instalado manualmente em ~/.jdk17 (padrão deste projeto).
    3. Lança RuntimeError orientando a instalação do Java.
    """
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home and Path(java_home, "bin", "java").exists():
        return java_home

    fallback = Path.home() / ".jdk17"
    if (fallback / "bin" / "java").exists():
        return str(fallback)

    raise RuntimeError(
        "JAVA_HOME não encontrado. Instale o JDK 17:\n"
        "  curl -sL https://github.com/adoptium/temurin17-binaries/releases/download/"
        "jdk-17.0.12%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.12_7.tar.gz"
        " | tar -xz -C ~/.jdk17 --strip-components=1"
    )


def get_spark_session(app_name: str = "AirflowHealthTechLakehouse"):
    """
    Inicializa a SparkSession com extensões Delta Lake sob demanda (Lazy Import).
    Evita inicialização prematura da JVM durante os ciclos de parsing do Scheduler.
    Injeta JAVA_HOME e PYSPARK_PYTHON no os.environ do worker antes de criar a sessão.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # Garante que JAVA_HOME e PATH estejam corretos no processo do worker
    java_home = _resolve_java_home()
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    # Garante que o worker do Airflow use o mesmo Python do .venv do projeto
    venv_python = Path(PROJECT_DIR) / ".venv" / "bin" / "python3"
    if venv_python.exists():
        os.environ.setdefault("PYSPARK_PYTHON", str(venv_python))

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
    # Re-injeta PROJECT_DIR no sys.path do worker (isolamento do Airflow 3)
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)

    from src.bronze import ingest_bronze

    spark = get_spark_session("Airflow_HealthTech_Bronze")
    spark.sparkContext.setLogLevel("WARN")

    total = ingest_bronze(spark, PATH_BRONZE, num_records=500)
    print(f"[Airflow Task] PROJECT_DIR={PROJECT_DIR}")
    print(f"[Airflow Task] Ingestão Bronze concluída. Total de registros: {total}")
    return total


def run_transform_silver(**context):
    """Executa a limpeza, deduplicação e cálculo de métricas clínicas na Silver."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)

    from src.silver import transform_silver

    spark = get_spark_session("Airflow_HealthTech_Silver")
    spark.sparkContext.setLogLevel("WARN")

    total = transform_silver(spark, PATH_BRONZE, PATH_SILVER)
    print(f"[Airflow Task] Transformação Silver concluída. Registros curados: {total}")
    return total


def run_load_gold(**context):
    """Executa a modelagem dimensional analítica (Fato e Dimensões) na Gold."""
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)

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
