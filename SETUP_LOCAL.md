# 🛠️ SETUP_LOCAL.md — Melhorias e Correções de Ambiente

> **Propósito:** Registrar todas as correções e ajustes necessários para rodar o projeto
> em qualquer máquina **além da máquina original do autor**. Serve de base para a PR
> de melhoria e para quem for contribuir com o projeto.

---

## 📋 Contexto

O `README.md` original documenta o passo a passo de instalação e execução,
porém ao seguir as instruções em um ambiente diferente do autor (`/home/ericl/projetos/`),
foram encontrados **4 problemas** que impediam a execução completa do projeto.

---

## 🐛 Bugs Encontrados e Correções Aplicadas

### Bug 1 — `python3 -m venv .venv` falha no Ubuntu/Debian com Python 3.14

**Sintoma:**
```
The virtual environment was not created successfully because ensurepip is not available.
apt install python3.14-venv
```

**Causa:** O Ubuntu/Debian separa o pacote `ensurepip` (necessário para criar venvs)
do interpretador Python base. O Python 3.14 foi instalado sem o módulo `venv`.

**Correção aplicada:**
Utilizar o [`virtualenv`](https://virtualenv.pypa.io/) como alternativa ao `python3 -m venv`,
que não depende do `ensurepip`:

```bash
# Instala virtualenv no espaço do usuário (sem sudo)
python3 -m pip install --user virtualenv --break-system-packages

# Cria o ambiente virtual
python3 -m virtualenv .venv

# Ativa o ambiente
source .venv/bin/activate

# Instala as dependências
pip install -r requirements.txt
```

> **Para a PR:** Adicionar no `README.md` uma seção de pré-requisitos com essa instrução
> alternativa para usuários do Ubuntu/Debian.

---

### Bug 2 — PySpark falha com `JAVA_GATEWAY_EXITED` (Java não instalado)

**Sintoma:**
```
JAVA_HOME is not set
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED]
Java gateway process exited before sending its port number.
```

**Causa:** O PySpark requer uma JVM (Java 8, 11 ou 17). A máquina não tinha Java instalado
e o `README.md` original não menciona Java como pré-requisito.

**Correção aplicada:**
Instalação manual do OpenJDK 17 (Temurin) sem necessidade de `sudo`:

```bash
# Cria o diretório e faz o download + extração do JDK 17
mkdir -p ~/.jdk17
curl -sL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.12%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.12_7.tar.gz" \
  | tar -xz -C ~/.jdk17 --strip-components=1

# Exporta as variáveis de ambiente (adicione ao ~/.zshrc ou ~/.bashrc para persistir)
export JAVA_HOME="$HOME/.jdk17"
export PATH="$JAVA_HOME/bin:$PATH"

# Verifica
java -version
# openjdk version "17.0.12" 2024-07-16
```

> **Para a PR:** Adicionar Java 17 como pré-requisito explícito no `README.md`.

---

### Bug 3 — DAG do Airflow falha com `ModuleNotFoundError: No module named 'src'`

**Sintoma (no log do Airflow em `~/airflow/logs/dag_id=.../attempt=2.log`):**
```
"exc_type":"ModuleNotFoundError",
"exc_value":"No module named 'src'",
"filename":"/home/pacod/airflow/dags/healthtech_lakehouse_dag.py",
"name":"run_ingest_bronze"
```

**Causa:** O arquivo `dags/healthtech_lakehouse_dag.py` tinha o caminho do projeto
**hardcoded** para a máquina do autor original:
```python
# ❌ Código original — hardcoded para a máquina do autor
PROJECT_DIR = "/home/ericl/projetos/healthtech-lakehouse"
```

Como o `PROJECT_DIR` errado era inserido no `sys.path`, o `import src.bronze` não
encontrava o pacote `src` do projeto real.

**Correção aplicada** em `dags/healthtech_lakehouse_dag.py`:
```python
# ✅ Resolução dinâmica via pathlib — funciona em qualquer máquina
from pathlib import Path

# __file__ = <PROJECT_DIR>/dags/healthtech_lakehouse_dag.py
# .parent   = <PROJECT_DIR>/dags/
# .parent.parent = <PROJECT_DIR>/
PROJECT_DIR = str(Path(__file__).resolve().parent.parent)
```

---

### Bug 4 — DAG do Airflow falha com `JAVA_GATEWAY_EXITED` (JAVA_HOME não herdado pelos workers)

**Causa:** O Airflow 3 (com `LocalExecutor`) inicia os workers como **subprocessos**
e não herda automaticamente as variáveis de ambiente exportadas no shell que iniciou
o `airflow standalone`. Assim, mesmo com `export JAVA_HOME=...` no terminal, o
worker não enxergava o Java.

**Correção aplicada** em `dags/healthtech_lakehouse_dag.py`:

```python
def _resolve_java_home() -> str:
    """Detecta o JAVA_HOME automaticamente, com fallback para ~/.jdk17."""
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home and Path(java_home, "bin", "java").exists():
        return java_home

    fallback = Path.home() / ".jdk17"
    if (fallback / "bin" / "java").exists():
        return str(fallback)

    raise RuntimeError("JAVA_HOME não encontrado. Instale o JDK 17...")


def get_spark_session(app_name: str = "AirflowHealthTechLakehouse"):
    # Injeta JAVA_HOME e PATH diretamente no os.environ do processo worker
    java_home = _resolve_java_home()
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    # Garante que o worker use o Python do .venv do projeto
    venv_python = Path(PROJECT_DIR) / ".venv" / "bin" / "python3"
    if venv_python.exists():
        os.environ.setdefault("PYSPARK_PYTHON", str(venv_python))
    ...
```

---

## ✅ Resultado Final — Todos os Passos Funcionando

| Passo | Comando | Status |
|:------|:--------|:------:|
| 1. Criar ambiente virtual | `python3 -m virtualenv .venv && source .venv/bin/activate` | ✅ |
| 2. Instalar dependências | `pip install -r requirements.txt` | ✅ |
| 3. Executar pipeline local | `python3 main.py` | ✅ Bronze/Silver/Gold: 500 registros |
| 4. Executar testes unitários | `python3 -m pytest tests/test_silver.py -v` | ✅ `1 passed in 35.88s` |
| 5. Iniciar dashboard Streamlit | `streamlit run app.py` | ✅ `http://localhost:8501` |
| 6. Executar DAG no Airflow 3 | `airflow standalone` + Trigger DAG | ✅ (após correções) |

---

## 🔧 Script de Setup Completo para Novos Contribuidores

```bash
# 1. Clonar o repositório
git clone https://github.com/loweri/healthtech-lakehouse.git
cd healthtech-lakehouse

# 2. Instalar Java 17 (sem sudo, via Temurin — pré-requisito do PySpark)
mkdir -p ~/.jdk17
curl -sL "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.12%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.12_7.tar.gz" \
  | tar -xz -C ~/.jdk17 --strip-components=1
export JAVA_HOME="$HOME/.jdk17"
export PATH="$JAVA_HOME/bin:$PATH"

# (Opcional) Persistir entre sessões — adicione ao ~/.zshrc ou ~/.bashrc:
# echo 'export JAVA_HOME="$HOME/.jdk17"' >> ~/.zshrc
# echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc

# 3. Criar o ambiente virtual
#    Nota: python3 -m venv falha em Debian/Ubuntu sem python3-venv instalado.
#    virtualenv é a alternativa portável que não depende de ensurepip.
python3 -m pip install --user virtualenv --break-system-packages
python3 -m virtualenv .venv
source .venv/bin/activate

# 4. Instalar dependências do projeto
pip install -r requirements.txt

# 5. Executar o pipeline local (Bronze → Silver → Gold)
python3 main.py

# 6. Executar a suíte de testes
python3 -m pytest tests/test_silver.py -v

# 7. Iniciar o dashboard Streamlit
streamlit run app.py
# Acesse: http://localhost:8501

# 8. Iniciar o Apache Airflow 3
mkdir -p ~/airflow/dags
cp dags/healthtech_lakehouse_dag.py ~/airflow/dags/
airflow standalone
# Usuário: admin
# Senha: cat ~/airflow/simple_auth_manager_passwords.json.generated
# Acesse: http://localhost:8080
# Trigger a DAG: healthtech_lakehouse_pyspark_pipeline
```

---

## 📦 Arquivos Modificados nesta Sessão

| Arquivo | Tipo de Mudança | Razão |
|:--------|:----------------|:------|
| `dags/healthtech_lakehouse_dag.py` | Correção de bug | `PROJECT_DIR` hardcoded + `JAVA_HOME` não injetado nos workers |
| `SETUP_LOCAL.md` | Novo arquivo | Documentação de setup e correções para contribuidores |

---

## 💡 Sugestões Adicionais para a PR

1. **Adicionar Java 17 como pré-requisito** no `README.md` (o PySpark exige JVM — não estava documentado).
2. **Adicionar instrução alternativa de venv** para Debian/Ubuntu com `virtualenv` (Bug 1).
3. **Criar um `Makefile`** com targets `make setup`, `make run`, `make test` para simplificar o onboarding.
4. **Adicionar `.env.example`** com `JAVA_HOME` documentado para facilitar a configuração.
5. **Parametrizar `num_records`** da DAG via Airflow Variables para flexibilidade operacional sem precisar editar o código.

---

*Sessão de diagnóstico e correção realizada em: 2026-08-17*
*Ambiente testado: Ubuntu/WSL2, Python 3.14.4, PySpark 4.1.1, Delta Lake 4.3.1, Airflow 3.3.1*
