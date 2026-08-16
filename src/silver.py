"""
silver.py — Camada Silver do HealthTech Data Lakehouse
=======================================================
Responsabilidade: Limpeza, deduplicação, normalização e engenharia de recursos
(Feature Engineering) sobre os dados brutos de internações hospitalares da Bronze.
Persiste a tabela curada em formato Delta Lake particionada por hospital_id.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, datediff, current_date, current_timestamp,
    when, round, lit
)


def transform_silver(spark: SparkSession, input_path: str, output_path: str) -> int:
    """
    Executa a esteira de transformação da Camada Silver:
      1. Leitura da Camada Bronze em formato Delta Lake.
      2. Deduplicação por identificador único de internação (admission_id).
      3. Conversão de tipos de dados (datas em formato DateType).
      4. Cálculo do tempo de permanência hospitalar (length_of_stay_days).
      5. Cálculo do custo total acumulado do tratamento (total_treatment_cost).
      6. Classificação de internação em cuidados críticos (is_critical_care).
      7. Persistência da tabela Silver Delta particionada por hospital_id.

    Args:
        spark: Instância ativa da SparkSession com suporte a Delta Lake.
        input_path: Caminho da tabela Delta de origem (Camada Bronze).
        output_path: Caminho de destino da tabela Delta curada (Camada Silver).

    Returns:
        Total de registros processados e persistidos na Camada Silver.
    """
    print(f"\n{'='*60}")
    print(f"  🔥 TRANSFORMAÇÃO SILVER — HealthTech Data Lakehouse")
    print(f"  📖 Lendo dados brutos da Bronze: {input_path}")
    print(f"{'='*60}\n")

    # 1. Carregar dados brutos da Camada Bronze
    df_bronze = spark.read.format("delta").load(input_path)

    # 2. Filtragem de integridade e deduplicação
    df_dedup = df_bronze \
        .filter(col("admission_id").isNotNull() & col("admission_date").isNotNull()) \
        .dropDuplicates(["admission_id"])

    # 3. Normalização de datas
    df_with_dates = df_dedup \
        .withColumn("admission_date_parsed", to_date(col("admission_date"), "yyyy-MM-dd")) \
        .withColumn("discharge_date_parsed", to_date(col("discharge_date"), "yyyy-MM-dd"))

    # 4. Engenharia de Recursos (Feature Engineering)
    # Define a data final de cálculo: data da alta ou a data atual (se ainda internado)
    df_transformed = df_with_dates \
        .withColumn(
            "effective_end_date",
            when(col("discharge_date_parsed").isNotNull(), col("discharge_date_parsed"))
            .otherwise(current_date())
        ) \
        .withColumn(
            "raw_days",
            datediff(col("effective_end_date"), col("admission_date_parsed"))
        ) \
        .withColumn(
            "length_of_stay_days",
            when(col("raw_days") < 1, 1).otherwise(col("raw_days"))
        ) \
        .withColumn(
            "total_treatment_cost",
            round(col("length_of_stay_days") * col("daily_cost"), 2)
        ) \
        .withColumn(
            "is_critical_care",
            when(col("bed_type") == "UTI", True).otherwise(False)
        ) \
        .withColumn(
            "is_currently_admitted",
            when(col("discharge_date").isNull(), True).otherwise(False)
        ) \
        .withColumn("silver_processed_timestamp", current_timestamp())

    # 5. Seleção e ordenação de colunas da Camada Silver
    df_silver = df_transformed.select(
        col("admission_id"),
        col("patient_name"),
        col("patient_cpf"),
        col("hospital_id"),
        col("hospital_name"),
        col("bed_type"),
        col("specialty"),
        col("cid_code"),
        col("cid_description"),
        col("admission_date_parsed").alias("admission_date"),
        col("discharge_date_parsed").alias("discharge_date"),
        col("is_currently_admitted"),
        col("is_critical_care"),
        col("length_of_stay_days"),
        col("daily_cost"),
        col("total_treatment_cost"),
        col("silver_processed_timestamp")
    )

    # 6. Gravação na Camada Silver (Delta Table Particionada)
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("hospital_id") \
        .save(output_path)

    total_silver = df_silver.count()

    print(f"\n  ✅ Camada Silver processada com sucesso!")
    print(f"  📁 Destino: {output_path}")
    print(f"  📊 Registros curados: {total_silver}")
    print(f"  🛏️  Internações críticas (UTI): {df_silver.filter(col('is_critical_care') == True).count()}")
    print(f"  🏥 Pacientes atualmente internados: {df_silver.filter(col('is_currently_admitted') == True).count()}")
    print(f"{'='*60}\n")

    return total_silver
