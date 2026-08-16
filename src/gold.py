"""
gold.py — Camada Gold do HealthTech Data Lakehouse
===================================================
Responsabilidade: Modelagem dimensional analítica (Data Warehouse / Lakehouse)
a partir dos dados curados da Camada Silver. Gera a Tabela Fato de Ocupação
Hospitalar (particionada por ano/mês) e as Tabelas Dimensão de Hospitais e Especialidades.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, avg, sum as spark_sum,
    count, round as spark_round, current_timestamp
)


def load_gold(spark: SparkSession, input_path: str, output_base_path: str) -> dict:
    """
    Executa o pipeline analítico da Camada Gold:
      1. Leitura da tabela Silver curada (Delta Lake).
      2. Criação da Tabela Fato (fact_hospital_occupancy) com partição temporal (year_month).
      3. Criação da Tabela Dimensão de Hospitais (dim_hospitals) com métricas consolidadas.
      4. Criação da Tabela Dimensão de Especialidades (dim_specialties).
      5. Persistência em Delta Lake com suporte a consultas analíticas de alta performance.

    Args:
        spark: Instância ativa da SparkSession com suporte a Delta Lake.
        input_path: Caminho da tabela Delta de origem (Camada Silver).
        output_base_path: Diretório base da Camada Gold (ex: storage/gold).

    Returns:
        Dicionário contendo a contagem de registros gerados em cada tabela analítica.
    """
    print(f"\n{'='*60}")
    print(f"  💎 MODELAGEM GOLD — HealthTech Data Lakehouse")
    print(f"  📖 Lendo dados curados da Silver: {input_path}")
    print(f"{'='*60}\n")

    # 1. Carregar dados da Camada Silver
    df_silver = spark.read.format("delta").load(input_path)

    # 2. TABELA FATO: fact_hospital_occupancy
    # Adiciona a coluna year_month para particionamento analítico temporal
    df_fact = df_silver \
        .withColumn("year_month", date_format(col("admission_date"), "yyyy-MM")) \
        .withColumn("gold_loaded_timestamp", current_timestamp())

    path_fact = f"{output_base_path}/fact_hospital_occupancy"
    df_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(path_fact)

    total_fact = df_fact.count()

    # 3. TABELA DIMENSÃO: dim_hospitals
    df_dim_hospitals = df_silver.groupBy("hospital_id", "hospital_name").agg(
        count("admission_id").alias("total_admissions"),
        spark_sum(col("is_critical_care").cast("int")).alias("total_icu_admissions"),
        spark_round(avg("length_of_stay_days"), 2).alias("avg_length_of_stay_days"),
        spark_round(spark_sum("total_treatment_cost"), 2).alias("total_spend_brl")
    ).withColumn("gold_loaded_timestamp", current_timestamp())

    path_dim_hospitals = f"{output_base_path}/dim_hospitals"
    df_dim_hospitals.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path_dim_hospitals)

    total_dim_hospitals = df_dim_hospitals.count()

    # 4. TABELA DIMENSÃO: dim_specialties
    df_dim_specialties = df_silver.groupBy("specialty").agg(
        count("admission_id").alias("total_patients"),
        spark_round(avg("length_of_stay_days"), 2).alias("avg_stay_days"),
        spark_round(avg("daily_cost"), 2).alias("avg_daily_cost_brl"),
        spark_round(spark_sum("total_treatment_cost"), 2).alias("total_cost_brl")
    ).withColumn("gold_loaded_timestamp", current_timestamp())

    path_dim_specialties = f"{output_base_path}/dim_specialties"
    df_dim_specialties.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path_dim_specialties)

    total_dim_specialties = df_dim_specialties.count()

    print(f"\n  ✅ Camada Gold consolidada com sucesso!")
    print(f"  🥇 Tabela Fato (fact_hospital_occupancy): {total_fact} registros")
    print(f"  🏥 Dimensão Hospitais (dim_hospitals): {total_dim_hospitals} registros")
    print(f"  🩺 Dimensão Especialidades (dim_specialties): {total_dim_specialties} registros")
    print(f"{'='*60}\n")

    return {
        "fact_hospital_occupancy": total_fact,
        "dim_hospitals": total_dim_hospitals,
        "dim_specialties": total_dim_specialties
    }
