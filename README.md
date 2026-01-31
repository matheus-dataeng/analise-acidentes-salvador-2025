# 🚦 Análise de Acidentes de Trânsito em Salvador (2025)

Projeto de **Engenharia de Dados** com foco na **análise dos acidentes de trânsito ocorridos em Salvador (BA) no ano de 2025**, utilizando pipelines orquestradas com **Apache Airflow**, ambiente conteinerizado com **Docker**, persistência em **PostgreSQL** e disponibilização dos dados via **Google Sheets**.


## 🎯 Objetivo do Projeto

O objetivo principal deste projeto é **analisar os acidentes de trânsito em Salvador no ano de 2025**, transformando dados brutos em uma base estruturada, confiável e pronta para análise.

Para atingir esse objetivo, foi desenvolvida uma arquitetura de dados que:

- Ingiere dados brutos de acidentes
- Realiza transformações e modelagem dos dados
- Armazena os dados tratados em um banco relacional
- Orquestra todo o fluxo com Airflow
- Disponibiliza os dados finais para consumo analítico

### 🔎 Exemplos de análises possíveis
- Tipos de acidentes mais frequentes em Salvador em 2025
- Horários e dias da semana com maior incidência
- Relação entre condição meteorológica e ocorrência de acidentes
- Impacto da visibilidade e fase do dia
- Distribuição de acidentes por localização


## 🧠 Contexto do Problema

Dados de acidentes de trânsito geralmente são disponibilizados de forma bruta, dificultando análises diretas.  
Este projeto resolve esse problema ao aplicar práticas de **engenharia de dados**, garantindo:

- Reprodutibilidade
- Padronização
- Orquestração
- Facilidade de consumo dos dados


## 🧱 Arquitetura da Solução

- **PostgreSQL**: Armazenamento dos dados tratados
- **Apache Airflow**: Orquestração dos pipelines
- **Docker Compose**: Padronização do ambiente
- **Google Sheets API**: Camada final de visualização e compartilhamento

Fluxo de dados:

Dados Brutos → PostgreSQL → Airflow (ETL) → Google Sheets


## 🔄 Pipeline de Dados

### DAG: `Analise_Acidentes_Salvador`

#### 1️⃣ Extract
- Consulta SQL no PostgreSQL
- Filtragem por município (Salvador) e UF (BA)
- Retorno dos dados via XCom em formato JSON

#### 2️⃣ Load
- Reconstrução do DataFrame a partir do XCom
- Autenticação via Service Account (Google Sheets)
- Escrita dos dados na planilha final

> Não há geração de arquivos intermediários (CSV ou Parquet).  
> A comunicação entre as tasks ocorre via **XCom**.


## ⚙️ Tecnologias Utilizadas

- Python
- Pandas
- SQL
- PostgreSQL
- SQLAlchemy
- Apache Airflow
- Docker 
- Google Sheets API

## ▶️ Como Executar o Projeto

### Pré-requisitos
- Docker
- Docker Compose
- Credenciais do Google Sheets (Service Account)

### Execução


docker compose up -d
Acesse a interface do Airflow:

http://localhost:8089
🔐 Variáveis de Ambiente
As variáveis sensíveis não são versionadas.
Exemplo de .env utilizado no projeto:

DB_USER=airflow
DB_PASSWORD=airflow
DB_HOST=postgres
DB_PORT=5432
DB_NAME=registro_acidentes

PATH_PRF=/opt/airflow/data/datatran2025.csv
As credenciais do Google Sheets são montadas via volume Docker e referenciadas por variável de ambiente.


## 📌 Boas Práticas Aplicadas

Separação clara entre DAGs e lógica de pipeline

Uso correto de XCom (sem arquivos intermediários)

Ambiente reprodutível com Docker

Proteção de credenciais via .gitignore

Projeto preparado para múltiplas DAGs


## 🚀 Possíveis Evoluções

Persistência em Data Lake (S3 / GCS)

Uso de formato Parquet

Criação de dashboards (Looker / Power BI)

Integração com BigQuery

Monitoramento e alertas no Airflow