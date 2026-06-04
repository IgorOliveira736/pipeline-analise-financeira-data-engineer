# pipeline-analise-financeira-data-engineer
Pipeline de dados financeiro em tempo real (Time-Series). Desenvolvido no Databricks Serverless utilizando a arquitetura Medallion para consumir a API da CoinGecko via Apache Spark (PySpark &amp; Spark SQL), com armazenamento em Delta Lake e orquestração automatizada via Databricks Workflows.

# ⚡ Pipeline de Engenharia de Dados: Crypto Time-Series Tracker com Apache Spark & API

## 📝 Descrição do Projeto
Desenvolvi este projeto para criar um pipeline de dados em tempo real focado no mercado financeiro de criptomoedas. O objetivo principal foi sair de cenários com arquivos estáticos e construir uma esteira automatizada que consome dados vivos de uma API externa, tratando-os e agregando-os em uma arquitetura de séries temporais (*Time-Series*).

O projeto foi construído no ambiente **Databricks Serverless**, utilizando **Apache Spark** (PySpark e Spark SQL) para o processamento distribuído e **Delta Lake** para gerenciar o histórico cumulativo das cotações do Bitcoin, Ethereum e Solana.

---

## 🏗️ Arquitetura do Pipeline & Fluxo de Dados

O projeto segue rigorosamente a Arquitetura Medallion, mas com uma abordagem voltada para streaming/batch incremental:

1. **Origem (API Externa):** Conexão direta com a API pública da **CoinGecko** para capturar métricas de mercado em formato JSON.
2. **Camada Bronze (Histórico Bruto):** Um notebook Python consome a API e faz o *append* (acumulação) dos dados brutos em formato Delta. Cada execução adiciona novas linhas, gerando uma linha do tempo das cotações.
3. **Camada Silver (Tratamento & Fuso Horário):** Limpeza de strings, seleção de colunas essenciais e a conversão crítica dos carimbos de data/hora globais para o fuso horário local.
4. **Camada Gold (Métricas de Volatilidade):** Utilização de Spark SQL e Window Functions para filtrar a cotação mais recente e calcular indicadores como a média móvel histórica de preços.
5. **Orquestração (Databricks Workflows):** Criação de um Job automatizado com um DAG (Gráfico Acíclico Direcionado) que executa toda a esteira de forma periódica e independente.

---

## 🧠 Desafios de Engenharia e Soluções Práticas
* **Restrições do Ambiente Serverless:** Inicialmente, tentei utilizar métodos que acessavam diretamente a JVM do Spark (`sparkContext.parallelize`), o que gerou um erro de permissão por conta da arquitetura compartilhada do Databricks Serverless. Contornei o problema utilizando o método nativo `spark.createDataFrame()` passando diretamente o dicionário Python, garantindo compatibilidade e performance.
* **Conflito Dinâmico de Tipos (`LongType` vs `DoubleType`):** Como os preços das moedas oscilam, a API começou a devolver o preço do Bitcoin como um número inteiro puro (`Long`) e o da Solana com casas decimais (`Double`), quebrando a inferência automática do Spark. Resolvi este problema criando um **esquema explícito estruturado** (`StructType`) para forçar o Spark a ler os campos numéricos como `Double`, blindando o pipeline.
* **Ajuste de Fuso Horário Local:** Os dados originais da API vinham no padrão UTC (3 horas adiantados em relação ao horário de Brasília). Na camada Silver, apliquei a função `from_utc_timestamp()` mapeada para o fuso `"America/Sao_Paulo"`, garantindo que os relatórios e dashboards finais exibissem a hora exata local correta.

---

## 🛠️ Tecnologias e Ferramentas Utilizadas
* **Ambiente:** Databricks Free Edition (Serverless Compute)
* **Motor de Big Data:** Apache Spark (Engine 3.x)
* **Linguagens:** Python (PySpark) e SQL (Spark SQL)
* **Orquestrador Nativo:** Databricks Jobs & Workflows
* **Armazenamento:** Delta Lake & Unity Catalog (Schema Isolado `crypto_analytics`)
* **Fonte de Dados:** CoinGecko Markets API (`/coins/markets`)

---

## 🧭 Estrutura dos Notebooks no Workflow

### 📍 1. Ingestão (`01_crypto_ingestion_bronze`)
* Conecta via biblioteca `requests` à API, valida o Status Code HTTP, aplica o Schema customizado e salva os novos registros na tabela `bronze_crypto` usando `mode("append")`.

### 📍 2. Transformação (`02_crypto_transform_silver`)
* Consome a Bronze, remove espaços com `trim()`, padroniza símbolos em maiúsculas com `upper()`, corrige o fuso horário para o horário de Brasília e salva os dados limpos por cima da tabela `silver_crypto` (`mode("overwrite")`).

### 📍 3. Indicadores Inteligentes (`03_crypto_analytics_gold`)
* Aplica a Window Function `ROW_NUMBER() OVER (PARTITION BY simbolo ORDER BY data_hora_extracao DESC)` para isolar a última foto do mercado. Calcula em paralelo a média de preço de todas as capturas acumuladas e classifica o risco de mercado com base na volatilidade percentual das últimas 24 horas.

---
*Projeto concluído, com a esteira de dados rodando de forma 100% autónoma e agendada, ideal para alimentar relatórios financeiros em tempo real.*
