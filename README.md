# 🏥 HealthTech Data Lakehouse — Hospital Analytics, PySpark & Delta Lake

![Python](https://img.shields.io/badge/Python-3.12%20%2F%203.14-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-4.3.1-00ADD8?logo=delta&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.3.1-017CEE?logo=apacheairflow&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.61.1-FF4B4B?logo=streamlit&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-6.9.0-3F4F75?logo=plotly&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-Testing_Suite-0A9EDC?logo=pytest&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

*(Bilingual Documentation: [Português](#-português) | [English](#-english))*

---

## 🇧🇷 Português

Este projeto implementa uma plataforma completa de **Data Lakehouse para Gestão Hospitalar & Cuidados Críticos (UTI)** sob a **Arquitetura Medalhão** (Bronze ➔ Silver ➔ Gold). A solução integra processamento distribuído massivo em **PySpark**, armazenamento colunar ACID resiliente em **Delta Lake**, orquestração automatizada no **Apache Airflow 3**, validação por suíte de testes em **Pytest** e um painel executivo interativo em **Streamlit & Plotly**.

---

### 📊 Painel Executivo de Gestão Hospitalar (Streamlit Analytics)

<p align="center">
  <img src="docs/streamlit_dashboard_1.png" alt="KPIs e Ranking de Ocupação de UTI por Hospital" width="100%">
</p>

<p align="center">
  <img src="docs/streamlit_dashboard_2.png" alt="Top Diagnósticos CID-10 e Proporção por Tipo de Leito" width="100%">
</p>

- **KPIs em Tempo Real:** Total de internações, taxa de ocupação de leitos de UTI (com alerta visual para saturação > 30%), tempo médio de permanência e custo total acumulado.
- **Análises Clínicas:** Ranking de hospitais com maior demanda crítica, distribuição de custos por especialidade médica, patologias mais frequentes (CID-10) e proporção por tipo de leito.

---

### ⚡ Orquestração no Apache Airflow 3

<p align="center">
  <img src="docs/airflow_execution.png" alt="Execução com Sucesso da DAG no Apache Airflow 3" width="100%">
</p>

- **DAG:** `healthtech_lakehouse_pyspark_pipeline` (Execução em 54 segundos com 100% de sucesso).
- **Padrão Lazy Imports:** Importação sob demanda das dependências da JVM para isolar tarefas e prevenir sobrecarga no Scheduler do Airflow.

---

### 🏗️ Tech Stack Blueprint: Arquitetura de Dados End-to-End

```mermaid
flowchart TD
    subgraph S1 ["🎲 ESTÁGIO 1: ORIGEM DOS DADOS"]
        FAKER["Biblioteca Faker (Python)\n• Prontuários Médicos & Pacientes (PT-BR)\n• Diagnósticos CID-10 & Tipos de Leito\n• Simulação de APIs Hospitalares REST"]
    end

    subgraph S2 ["🥉 ESTÁGIO 2: INGESTÃO BRUTA (CAMADA BRONZE)"]
        SCHEMA["Schema Enforcement Estrito (StructType)\nValidação de Tipos & Nullable"]
        DELTA_BRONZE["storage/bronze/\n(Delta Table · Partição: hospital_id)\nTransaction Log ACID (_delta_log)"]
        FAKER -->|Dados Brutos| SCHEMA
        SCHEMA -->|Gravação Atômica| DELTA_BRONZE
    end

    subgraph S3 ["🥈 ESTÁGIO 3: LIMPEZA & ENRIQUECIMENTO (CAMADA SILVER)"]
        PY_SILVER["src/silver.py (PySpark Engine)\n• Deduplicação (dropDuplicates por admission_id)\n• Normalização de Datas (to_date YYYY-MM-DD)\n• Feature Engineering: Dias de Permanência\n• Cálculo do Custo de Tratamento & Flag UTI"]
        DELTA_SILVER["storage/silver/\n(Delta Table · Partição: hospital_id)"]
        DELTA_BRONZE --> PY_SILVER
        PY_SILVER -->|Delta Overwrite Particionado| DELTA_SILVER
    end

    subgraph S4 ["🥇 ESTÁGIO 4: DATA WAREHOUSE & MODELAGEM (CAMADA GOLD)"]
        PY_GOLD["src/gold.py (Modelagem Star Schema / Kimball)"]
        FACT["🥇 storage/gold/fact_hospital_occupancy/\n(Tabela Fato · Partição: year_month)"]
        DIM_HOSP["dim_hospitals (Capacidade & Gastos)"]
        DIM_SPEC["dim_specialties (Custo por Especialidade)"]

        DELTA_SILVER --> PY_GOLD
        PY_GOLD --> FACT
        PY_GOLD --> DIM_HOSP
        PY_GOLD --> DIM_SPEC
    end

    subgraph S5 ["⚡ ESTÁGIO 5: ORQUESTRAÇÃO"]
        AIRFLOW["Apache Airflow 3 (Web UI)\nDAG: healthtech_lakehouse_pyspark_pipeline\n(Lazy Imports & Políticas de Retry)"]
        AIRFLOW -.->|Task 1| S2
        AIRFLOW -.->|Task 2| S3
        AIRFLOW -.->|Task 3| S4
    end

    subgraph S6 ["🛡️ ESTÁGIO 6: QUALIDADE & TESTES"]
        PYTEST["Pytest Testing Suite (tests/test_silver.py)\n• Padrão AAA (Arrange, Act, Assert)\n• Fixtures de Spark & Diretórios Temporários\n• Validação de Contratos Clínicos"]
    end

    subgraph S7 ["📊 ESTÁGIO 7: CONSUMO & ANALYTICS"]
        STREAMLIT["Streamlit & Plotly (app.py)\n• Painel Executivo Dark Mode\n• Filtros Dinâmicos por Hospital e Especialidade"]
        FACT --> STREAMLIT
        DIM_HOSP --> STREAMLIT
        DIM_SPEC --> STREAMLIT
    end
```

---

### 💡 Decisões Técnicas de Engenharia

| Decisão de Arquitetura | Justificativa Técnica |
| :--- | :--- |
| **Delta Lake (Transações ACID)** | Garante atomicidade em gravações distribuídas. Se uma tarefa falhar no meio, o commit não é registrado no `_delta_log` (Rollback automático). |
| **Schema Enforcement (`StructType`)** | Contrato de dados estrito na ingestão Bronze. Bloqueia corrupção caso o sistema hospitalar envie formatos inválidos. |
| **Particionamento Inteligente (*Partition Pruning*)** | A Bronze e Silver são particionadas por `hospital_id` (baixa cardinalidade), enquanto a Tabela Fato na Gold é particionada por `year_month` para acelerar relatórios temporais em até 90%. |
| **Modelagem Star Schema (Kimball)** | Separação estrita entre métricas numéricas agregáveis (`fact_hospital_occupancy` no centro) e tabelas de contexto descritivo (`dim_hospitals`, `dim_specialties` ao redor). |
| **Lazy Imports no Airflow 3** | Mover as importações de PySpark para dentro dos callables das tarefas evita que o Scheduler inicialize a JVM em cada ciclo de parsing (a cada 30 segundos). |
| **Padrão AAA nos Testes Unitários** | Testes isolados com `pytest` utilizando pastas descartáveis (`tmp_path`) e asserções estritas com `assert` sem impactar os dados reais de produção. |
| **`PROJECT_DIR` Dinâmico na DAG (Portabilidade)** | O path raiz do projeto é resolvido via cascata: variável de ambiente `HEALTHTECH_PROJECT_DIR` → `Path(__file__).parent.parent` → fallback `~/github/healthtech-lakehouse`. Elimina o path hardcoded que causava `ModuleNotFoundError: No module named 'src'` em qualquer máquina diferente da do autor. |
| **`JAVA_HOME` Injetado via `os.environ` na DAG** | O Airflow 3 (`LocalExecutor`) inicia workers como subprocessos que não herdam variáveis do shell. O `JAVA_HOME` é injetado programaticamente em `os.environ` dentro do callable, garantindo que a JVM seja encontrada independentemente de como o Airflow foi iniciado. |
| **`dags_folder` Apontando para o Repositório** | Em vez de copiar o DAG para `~/airflow/dags/`, o `airflow.cfg` aponta diretamente para `<repo>/dags/`. Isso garante que `__file__` dentro do DAG resolva para o path correto do repositório, evitando quebra do `sys.path`. |

---

### 📂 Estrutura do Repositório

```text
healthtech-lakehouse/
├── .venv/                         # Ambiente Virtual Local
├── README.md                      # Documentação completa do projeto
├── SETUP_LOCAL.md                 # Guia de setup local e registro de correções (PR)
├── requirements.txt               # Dependências do projeto
├── main.py                        # Ponto de entrada para execução e validação local
├── app.py                         # Painel Executivo Interativo (Streamlit & Plotly)
│
├── dags/
│   └── healthtech_lakehouse_dag.py # DAG de orquestração no Apache Airflow 3
│
├── src/
│   ├── __init__.py                # Pacote Python
│   ├── bronze.py                  # Ingestão Bronze com Schema Enforcement
│   ├── silver.py                  # Limpeza, deduplicação e Feature Engineering
│   └── gold.py                    # Modelagem dimensional Star Schema (Fato & Dimensões)
│
├── tests/
│   └── test_silver.py             # Suíte de testes unitários automatizados (Pytest)
│
├── docs/
│   ├── airflow_execution.png      # Evidência de execução 100% verde no Airflow
│   ├── streamlit_dashboard_1.png  # Evidência do Dashboard (KPIs e Rankings)
│   └── streamlit_dashboard_2.png  # Evidência do Dashboard (CIDs e Leitos)
│
└── storage/                       # Data Lakehouse Local (Delta Lake Tables)
    ├── bronze/                    # Dados brutos particionados por hospital_id
    ├── silver/                    # Dados curados particionados por hospital_id
    └── gold/                      # Tabela Fato (year_month) e Dimensões
```

---

### 🚀 Como Executar o Projeto

> **Dica:** Para um guia detalhado de setup em ambientes Debian/Ubuntu e registro completo de correções, consulte o [SETUP_LOCAL.md](./SETUP_LOCAL.md).

#### Pré-requisitos

| Dependência | Versão | Notas |
| :--- | :--- | :--- |
| Python | 3.12+ / 3.14 | Testado com 3.14.4 |
| **Java (JDK)** | **17** | **Obrigatório para PySpark. Não incluído no `requirements.txt`.** |
| `virtualenv` | qualquer | Necessário em Debian/Ubuntu — substitui `python3 -m venv` (ver passo 1) |

#### 1. Clonar o repositório e preparar o ambiente

```bash
git clone https://github.com/loweri/healthtech-lakehouse.git
cd healthtech-lakehouse
```

**Instalar o Java 17** (pré-requisito do PySpark — sem necessidade de `sudo`):
```bash
mkdir -p ~/.jdk17
curl -sL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.12%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.12_7.tar.gz" \
  | tar -xz -C ~/.jdk17 --strip-components=1
export JAVA_HOME="$HOME/.jdk17"
export PATH="$JAVA_HOME/bin:$PATH"
```

> Adicione as duas linhas `export` ao `~/.bashrc` ou `~/.zshrc` para persistir entre sessões.

**Criar o ambiente virtual e instalar dependências:**

```bash
# Em sistemas Debian/Ubuntu, o módulo 'venv' pode não estar disponível.
# Use 'virtualenv' como alternativa portável:
python3 -m pip install --user virtualenv --break-system-packages
python3 -m virtualenv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

#### 2. Executar o Pipeline Localmente
```bash
python3 main.py
```

#### 3. Executar os Testes Unitários
```bash
python3 -m pytest tests/test_silver.py -v
```

#### 4. Iniciar o Painel Streamlit
```bash
streamlit run app.py
```
Acesse `http://localhost:8501` no navegador.

#### 5. Executar no Apache Airflow 3

O DAG já está em `dags/` do repositório. Configure o Airflow para lê-lo diretamente, sem necessidade de copiar o arquivo:

```bash
# Aponta o dags_folder diretamente para o repositório (execute uma única vez)
sed -i "s|dags_folder = .*|dags_folder = $(pwd)/dags|" ~/airflow/airflow.cfg

# Iniciar o Airflow com as variáveis necessárias
export JAVA_HOME="$HOME/.jdk17"
export PATH="$JAVA_HOME/bin:$PATH"
export HEALTHTECH_PROJECT_DIR="$(pwd)"

airflow standalone
```

Acesse `http://localhost:8080`. A senha do admin está em:
```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```
Acesse a DAG `healthtech_lakehouse_pyspark_pipeline` e clique em **Trigger DAG** ▶️.

---

## 🇺🇸 English

Enterprise-grade **HealthTech Data Lakehouse** built upon the **Medallion Architecture** (Bronze ➔ Silver ➔ Gold). The platform couples distributed processing via **Apache Spark (PySpark)** with ACID guarantees and Time Travel features of **Delta Lake**, fully orchestrated by **Apache Airflow 3**, validated through **Pytest** automated testing, and visualized via an interactive **Streamlit & Plotly** executive dashboard.

> **Setup Guide:** See [SETUP_LOCAL.md](./SETUP_LOCAL.md) for a complete local setup guide, including Debian/Ubuntu-specific instructions and a record of all bug fixes applied.

### 🌟 Key Highlights

- **Distributed Data Engine:** PySpark for processing large-scale hospital admission records without memory bottlenecks.
- **ACID Transaction Log:** Delta Lake table format enabling reliable writes, schema enforcement, and time travel capabilities.
- **Dimensional Modeling (Star Schema):** Curated Fact Table (`fact_hospital_occupancy`) partitioned by `year_month` coupled with dimension tables (`dim_hospitals`, `dim_specialties`).
- **Airflow 3 Orchestration:** DAG utilizing Lazy Imports for lightweight scheduler parsing cycles and robust task dependency management. `PROJECT_DIR` resolved dynamically (portable across machines). `JAVA_HOME` injected at runtime into worker `os.environ`.
- **Automated Unit Testing:** Pytest suite with isolated Spark fixtures validating length of stay calculations, ICU flags, and cost metrics.
- **Executive Analytics:** Streamlit dark-mode dashboard with dynamic hospital filters, ICU capacity alerts, and diagnostic distributions.
- **Java 17 (JDK):** Required by PySpark — install via [Eclipse Temurin](https://adoptium.net/) without `sudo` (see setup guide).

---

## 👨‍💻 Autor / Author

**Ericles Fernandes Oliveira** — *Data Engineer*  
GitHub: [loweri](https://github.com/loweri) | LinkedIn: [ericlesoliveira](https://www.linkedin.com/in/ericlesoliveira/)
