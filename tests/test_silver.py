"""
test_silver.py — Suíte de Testes Unitários da Camada Silver (Pytest)
====================================================================
Responsabilidade: Validação isolada de integridade, deduplicação e
regras de negócio clínicas com dados sintéticos e diretórios temporários.
"""

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from delta import configure_spark_with_delta_pip

# Garante a importação do pacote src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.silver import transform_silver


@pytest.fixture(scope="session")
def spark():
    """
    Fixture que inicializa uma SparkSession local isolada e leve com Delta Lake.
    """
    builder = SparkSession.builder \
        .appName("HealthTechPyTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .master("local[1]")

    return configure_spark_with_delta_pip(builder).getOrCreate()


def test_transform_silver_pipeline(spark, tmp_path):
    """
    Testa a função transform_silver verificando:
      - Deduplicação de admission_id.
      - Cálculo correto de dias de internação (length_of_stay_days).
      - Garantia de permanência mínima de 1 dia para altas no mesmo dia.
      - Cálculo correto de custo total acumulado (total_treatment_cost).
      - Flag de cuidados críticos (is_critical_care) para UTI.
      - Identificação de pacientes atualmente internados (is_currently_admitted).
    """
    path_bronze = str(tmp_path / "bronze")
    path_silver = str(tmp_path / "silver")

    # 1. Definir schema de teste compatível com a Bronze
    schema = StructType([
        StructField("admission_id",        StringType(),    False),
        StructField("patient_name",        StringType(),    True),
        StructField("patient_cpf",         StringType(),    True),
        StructField("hospital_id",         StringType(),    False),
        StructField("hospital_name",       StringType(),    True),
        StructField("bed_type",            StringType(),    True),
        StructField("specialty",           StringType(),    True),
        StructField("cid_code",            StringType(),    True),
        StructField("cid_description",     StringType(),    True),
        StructField("admission_date",      StringType(),    True),
        StructField("discharge_date",      StringType(),    True),
        StructField("daily_cost",          DoubleType(),    True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    # 2. Injetar dados sintéticos com cenários de borda
    data = [
        # Paciente 1: 4 dias em UTI (R$ 5.000/dia -> Total R$ 20.000)
        ("ADM-001", "Paciente A", "111.111.111-11", "H001", "Hospital SP",
         "UTI", "Cardiologia", "I21", "Infarto", "2026-08-01", "2026-08-05", 5000.0, datetime.now()),

        # Paciente 2: Alta no mesmo dia (mínimo 1 dia em Enfermaria -> Total R$ 1.000)
        ("ADM-002", "Paciente B", "222.222.222-22", "H001", "Hospital SP",
         "Enfermaria", "Ortopedia", "S72", "Fratura", "2026-08-10", "2026-08-10", 1000.0, datetime.now()),

        # Paciente 3: Ainda internado (discharge_date = None) em Semi-Intensiva
        ("ADM-003", "Paciente C", "333.333.333-33", "H002", "Hospital RJ",
         "Semi-Intensiva", "Pneumonia", "J18", "Pneumonia", "2026-08-01", None, 2000.0, datetime.now()),

        # Paciente 4: REGISTRO DUPLICADO do Paciente 1 (deve ser descartado pela deduplicação)
        ("ADM-001", "Paciente A", "111.111.111-11", "H001", "Hospital SP",
         "UTI", "Cardiologia", "I21", "Infarto", "2026-08-01", "2026-08-05", 5000.0, datetime.now()),
    ]

    df_test_bronze = spark.createDataFrame(data, schema=schema)
    df_test_bronze.write.format("delta").save(path_bronze)

    # 3. Executar a função real da Silver
    total_silver = transform_silver(spark, path_bronze, path_silver)

    # 4. Asserções do Teste Unitário
    # Deve conter exatamente 3 registros (o 4º duplicado foi removido)
    assert total_silver == 3, "A deduplicação falhou: deveria ter exatamente 3 registros curados."

    # Ler a Delta Table Silver gerada para validação dos cálculos
    df_result = spark.read.format("delta").load(path_silver)

    # Validação Paciente 1 (UTI - 4 dias)
    row_p1 = df_result.filter(df_result.admission_id == "ADM-001").collect()[0]
    assert row_p1.length_of_stay_days == 4, "Tempo de permanência do Paciente 1 deveria ser 4 dias."
    assert row_p1.total_treatment_cost == 20000.0, "Custo total do Paciente 1 deveria ser R$ 20.000,00."
    assert row_p1.is_critical_care is True, "Paciente 1 em UTI deveria ter is_critical_care = True."
    assert row_p1.is_currently_admitted is False, "Paciente 1 já teve alta."

    # Validação Paciente 2 (Alta no mesmo dia -> mínimo 1 dia)
    row_p2 = df_result.filter(df_result.admission_id == "ADM-002").collect()[0]
    assert row_p2.length_of_stay_days == 1, "Alta no mesmo dia deve contar como permanência mínima de 1 dia."
    assert row_p2.total_treatment_cost == 1000.0, "Custo total do Paciente 2 deveria ser R$ 1.000,00."
    assert row_p2.is_critical_care is False, "Enfermaria não é UTI (deve ser False)."

    # Validação Paciente 3 (Ainda internado)
    row_p3 = df_result.filter(df_result.admission_id == "ADM-003").collect()[0]
    assert row_p3.is_currently_admitted is True, "Paciente 3 sem data de alta deve ter is_currently_admitted = True."
    assert row_p3.length_of_stay_days >= 1, "Paciente ainda internado deve ter pelo menos 1 dia calculado."
