"""
main.py — Ponto de Entrada para Execução Local do Pipeline HealthTech
====================================================================
Este script permite a execução manual e validação das camadas do
HealthTech Data Lakehouse em ambiente de desenvolvimento.
"""

import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.bronze import ingest_bronze


def get_spark_session(app_name: str = "HealthTechLakehouse") -> SparkSession:
    """
    Inicializa e configura uma SparkSession local com suporte nativo ao Delta Lake.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .master("local[*]")

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    """Executa o fluxo de ingestão da Camada Bronze."""
    print("\n" + "=" * 70)
    print("  🚀 INICIANDO EXECUÇÃO LOCAL DO HEALTHTECH DATA LAKEHOUSE")
    print("=" * 70 + "\n")

    # 1. Obter SparkSession com Delta Lake
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir caminhos de armazenamento
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path_bronze = os.path.join(base_dir, "storage", "bronze")

    # 3. Executar Ingestão Bronze
    total_records = ingest_bronze(spark, path_bronze, num_records=500)

    # 4. Inspecionar e validar os dados gravados na Bronze
    print("\n🔍 Inspecionando os primeiros 5 registros gravados no Delta Lake (Bronze):")
    df_read_bronze = spark.read.format("delta").load(path_bronze)
    df_read_bronze.select(
        "admission_id", "patient_name", "hospital_id",
        "bed_type", "specialty", "daily_cost"
    ).show(5, truncate=False)

    print("\n📊 Contagem de pacientes por Hospital na Bronze:")
    df_read_bronze.groupBy("hospital_id", "hospital_name").count().show(truncate=False)

    print("\n" + "=" * 70)
    print(f"  ✨ PIPELINE BRONZE CONCLUÍDO COM SUCESSO! ({total_records} registros)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
