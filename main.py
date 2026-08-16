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
from src.silver import transform_silver


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
    """Executa o fluxo completo Bronze -> Silver do HealthTech Lakehouse."""
    print("\n" + "=" * 70)
    print("  🚀 INICIANDO EXECUÇÃO LOCAL DO HEALTHTECH DATA LAKEHOUSE")
    print("=" * 70 + "\n")

    # 1. Obter SparkSession com Delta Lake
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir caminhos de armazenamento
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path_bronze = os.path.join(base_dir, "storage", "bronze")
    path_silver = os.path.join(base_dir, "storage", "silver")

    # 3. Executar Ingestão Bronze
    total_bronze = ingest_bronze(spark, path_bronze, num_records=500)

    # 4. Executar Transformação Silver
    total_silver = transform_silver(spark, path_bronze, path_silver)

    # 5. Inspecionar e validar os dados enriquecidos na Silver
    print("\n🔍 Inspecionando 5 registros curados na Camada Silver (com métricas calculadas):")
    df_read_silver = spark.read.format("delta").load(path_silver)
    df_read_silver.select(
        "admission_id", "hospital_id", "bed_type",
        "length_of_stay_days", "daily_cost", "total_treatment_cost",
        "is_critical_care", "is_currently_admitted"
    ).show(5, truncate=False)

    print("\n📊 Métricas Agregadas por Especialidade na Silver:")
    df_read_silver.groupBy("specialty").agg(
        {"length_of_stay_days": "avg", "total_treatment_cost": "sum"}
    ).withColumnRenamed("avg(length_of_stay_days)", "media_dias_internado") \
     .withColumnRenamed("sum(total_treatment_cost)", "custo_total_especialidade") \
     .show(truncate=False)

    print("\n" + "=" * 70)
    print(f"  ✨ PIPELINE BRONZE & SILVER CONCLUÍDO COM SUCESSO! ({total_silver} registros)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
