"""
bronze.py — Camada Bronze do HealthTech Data Lakehouse
=======================================================
Responsabilidade: Gerar dados sintéticos estruturados de internações hospitalares
utilizando a biblioteca Faker e persistir na camada Bronze em formato Delta Lake
com Schema Enforcement estrito (StructType) e particionamento por hospital_id.
"""

import os
import random
from datetime import datetime, timedelta

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)
from delta import configure_spark_with_delta_pip


# ===========================================================================
# CONFIGURAÇÕES DO DOMÍNIO HOSPITALAR (Tabelas de Referência)
# ===========================================================================

HOSPITALS = {
    "H001": "Hospital São Lucas - São Paulo",
    "H002": "Hospital Santa Casa - Rio de Janeiro",
    "H003": "Hospital Albert Sabin - Belo Horizonte",
    "H004": "Hospital Getúlio Vargas - Recife",
    "H005": "Hospital Moinhos de Vento - Porto Alegre",
}

BED_TYPES = ["UTI", "Enfermaria", "Semi-Intensiva"]

SPECIALTIES = [
    "Cardiologia", "Neurologia", "Ortopedia",
    "Pneumologia", "Oncologia", "Pediatria",
]

CID_CODES = {
    "I21":   "Infarto Agudo do Miocárdio",
    "J18":   "Pneumonia",
    "S72":   "Fratura do Fêmur",
    "I63":   "AVC Isquêmico",
    "C34":   "Neoplasia Maligna dos Brônquios e Pulmão",
    "K35":   "Apendicite Aguda",
    "J44":   "Doença Pulmonar Obstrutiva Crônica (DPOC)",
    "N39":   "Infecção do Trato Urinário",
    "E11":   "Diabetes Mellitus Tipo 2",
    "A09":   "Gastroenterite Infecciosa",
}

DAILY_COST_RANGES = {
    "UTI":             (2500.00, 8000.00),
    "Semi-Intensiva":  (1200.00, 3500.00),
    "Enfermaria":      (400.00, 1500.00),
}


# ===========================================================================
# CONTRATO DE DADOS: SCHEMA DA CAMADA BRONZE (Schema Enforcement)
# ===========================================================================

BRONZE_SCHEMA = StructType([
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


# ===========================================================================
# FUNÇÕES DE GERAÇÃO DE DADOS SINTÉTICOS (Faker)
# ===========================================================================

def generate_hospital_admissions(num_records: int = 500, seed: int = 42) -> list[dict]:
    """
    Gera registros sintéticos de internações hospitalares para simulação
    de ingestão de dados em ambiente de desenvolvimento.

    Args:
        num_records: Volume de registros a serem gerados.
        seed: Semente pseudoaleatória para reprodutibilidade dos dados.

    Returns:
        Lista de dicionários formatados conforme o BRONZE_SCHEMA.
    """
    fake = Faker("pt_BR")
    Faker.seed(seed)
    random.seed(seed)

    admissions = []
    now = datetime.now()

    for i in range(num_records):
        hospital_id = random.choice(list(HOSPITALS.keys()))
        bed_type = random.choice(BED_TYPES)
        specialty = random.choice(SPECIALTIES)
        cid_code = random.choice(list(CID_CODES.keys()))

        admission_date = fake.date_between(start_date="-12m", end_date="today")

        if random.random() < 0.80:
            days_admitted = random.randint(1, 30)
            discharge_date = admission_date + timedelta(days=days_admitted)
            if discharge_date > now.date():
                discharge_date = None
        else:
            discharge_date = None

        cost_min, cost_max = DAILY_COST_RANGES[bed_type]
        daily_cost = round(random.uniform(cost_min, cost_max), 2)

        admission = {
            "admission_id":        f"ADM-{i+1:06d}",
            "patient_name":        fake.name(),
            "patient_cpf":         fake.cpf(),
            "hospital_id":         hospital_id,
            "hospital_name":       HOSPITALS[hospital_id],
            "bed_type":            bed_type,
            "specialty":           specialty,
            "cid_code":            cid_code,
            "cid_description":     CID_CODES[cid_code],
            "admission_date":      str(admission_date),
            "discharge_date":      str(discharge_date) if discharge_date else None,
            "daily_cost":          daily_cost,
            "ingestion_timestamp": now,
        }
        admissions.append(admission)

    return admissions


# ===========================================================================
# FUNÇÃO PRINCIPAL: INGESTÃO NA CAMADA BRONZE (Delta Lake)
# ===========================================================================

def ingest_bronze(spark: SparkSession, output_path: str, num_records: int = 500) -> int:
    """
    Executa a ingestão da camada Bronze, validando os dados com Schema Enforcement
    e persistindo a tabela Delta particionada por hospital_id.

    Args:
        spark: Instância ativa da SparkSession configurada com Delta Lake.
        output_path: Diretório de destino no storage do Data Lakehouse.
        num_records: Volume de registros para a ingestão.

    Returns:
        Total de registros persistidos na camada Bronze.
    """
    print(f"\n{'='*60}")
    print(f"  🏥 INGESTÃO BRONZE — HealthTech Data Lakehouse")
    print(f"  📊 Gerando {num_records} registros de internações hospitalares...")
    print(f"{'='*60}\n")

    raw_data = generate_hospital_admissions(num_records)
    df_bronze = spark.createDataFrame(raw_data, schema=BRONZE_SCHEMA)

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("hospital_id") \
        .save(output_path)

    total = df_bronze.count()

    print(f"\n  ✅ Bronze gravada com sucesso!")
    print(f"  📁 Caminho: {output_path}")
    print(f"  📊 Total de registros: {total}")
    print(f"  🏥 Hospitais particionados: {df_bronze.select('hospital_id').distinct().count()}")
    print(f"  🛏️  Tipos de leito: {df_bronze.select('bed_type').distinct().count()}")
    print(f"{'='*60}\n")

    return total
