"""
main.py — Ponto de Entrada para Execução Local do Pipeline HealthTech
====================================================================
Este script executa e valida o pipeline completo da Arquitetura Medalhão:
  Bronze (Raw Ingestion) -> Silver (Curated) -> Gold (Analytical Data Warehouse)
"""

import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.bronze import ingest_bronze
from src.silver import transform_silver
from src.gold import load_gold


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
    """Executa o pipeline Medalhão completo do HealthTech Lakehouse."""
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
    path_gold = os.path.join(base_dir, "storage", "gold")

    # 3. Executar Camadas Medalhão
    total_bronze = ingest_bronze(spark, path_bronze, num_records=500)
    total_silver = transform_silver(spark, path_bronze, path_silver)
    gold_counts = load_gold(spark, path_silver, path_gold)

    # 4. RESPOSTAS ÀS PERGUNTAS DE NEGÓCIO DO EXCALIDRAW:
    print("\n" + "=" * 70)
    print("  🎯 RESPOSTAS ÀS PERGUNTAS DE NEGÓCIO (DIRETAMENTE DA CAMADA GOLD)")
    print("=" * 70)

    # Pergunta 1: Qual hospital está com mais internações de UTI?
    print("\n🏨 PERGUNTA 1: Ranking de Hospitais por Internações Críticas (UTI):")
    df_hospitals = spark.read.format("delta").load(f"{path_gold}/dim_hospitals")
    df_hospitals.select(
        "hospital_id", "hospital_name", "total_icu_admissions",
        "total_admissions", "total_spend_brl"
    ).orderBy("total_icu_admissions", ascending=False).show(truncate=False)

    # Pergunta 2: Qual o tempo médio de internação por especialidade?
    print("\n🩺 PERGUNTA 2: Tempo Médio de Internação e Custo por Especialidade:")
    df_specialties = spark.read.format("delta").load(f"{path_gold}/dim_specialties")
    df_specialties.select(
        "specialty", "total_patients", "avg_stay_days", "total_cost_brl"
    ).orderBy("avg_stay_days", ascending=False).show(truncate=False)

    # Pergunta 3: Quanto estamos gastando por mês com internações?
    print("\n💰 PERGUNTA 3: Evolução dos Gastos Hospitalares por Mês (Partição year_month):")
    df_fact = spark.read.format("delta").load(f"{path_gold}/fact_hospital_occupancy")
    df_fact.groupBy("year_month").agg(
        {"total_treatment_cost": "sum", "admission_id": "count"}
    ).withColumnRenamed("sum(total_treatment_cost)", "gasto_total_mes_brl") \
     .withColumnRenamed("count(admission_id)", "total_internacoes") \
     .orderBy("year_month").show(15, truncate=False)

    print("\n" + "=" * 70)
    print("  ✨ PIPELINE MEDALHÃO COMPLETO EXECUTADO COM SUCESSO! 🟢")
    print(f"  🥉 Bronze: {total_bronze} | 🥈 Silver: {total_silver} | 🥇 Gold: {gold_counts['fact_hospital_occupancy']}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
