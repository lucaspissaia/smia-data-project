# 🦟 SMIA: Sistema de Mapeamento Inteligente de Arboviroses
**Transformando a gestão de saúde pública de reativa para preditiva.**

![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-green)
![GCP](https://img.shields.io/badge/AWS-Native-blue)
![Top2](https://img.shields.io/badge/Award-Top_2_FIAP_Discovery-gold)

---

## 📌 O Problema e o Impacto de Negócio
Atualmente, o Brasil gasta bilhões anualmente com ações reativas contra arboviroses (Dengue, Zika, Chikungunya). O sistema oficial de saúde funciona como um "espelho retrovisor": a notificação ocorre dias após o contágio, quando o surto já está em andamento.

**O objetivo do SMIA é atuar como um "GPS":** cruzar dados históricos de infecção com dados climáticos para prever surtos com semanas de antecedência, permitindo que a gestão pública atue de forma preventiva e otimize o uso de verbas.

**Escopo do projeto atual:** como um MVP (minimum Viable Product) focaremos em Dengue para a cidade de São Paulo.

---

## 🏗️ Arquitetura da Solução (Data de Ponta a Ponta)
![Diagrama de Arquitetura](./docs/Diagrama%20de%20Arquitetura.png)

O projeto foi estruturado em três pilares principais, refletindo o ciclo de vida completo do dado:

### 1. ⚙️ Engenharia de Dados (Extração e Data Lake)
Responsável por orquestrar a coleta diária e automatizada de dados brutos.
- **Fontes:** API INMET (Clima) e SINAN/DataSUS (Saúde).
- **Ferramentas:** Python, AWS Lambda, EventBridge.
- **Destino:** Amazon S3 (Camada Bronze).
- 📁 [Acesse os códigos de Engenharia](./data-engineering/)

### 2. 📊 Analytics Engineering (Modelagem e DW)
Transformação dos dados brutos em um modelo dimensional otimizado para análises e machine learning.
- **Ferramentas:** Databricks, PySpark, SQL.
- **Camadas:** Silver (Limpeza e Padronização) e Gold (Tabelas Fato e Dimensão) -> A depender da modelagem de dados e diagrama de entidades.
- 📁 [Acesse os modelos do dbt](./analytics-engineering/)

### 3. 🧠 Data Science & Analytics (Predição e Consumo)
Geração de insights de negócio e modelagem preditiva de surtos.
- **Ferramentas:** Amazon Quicksight, Python, MLFlow (Machine Learning).
- 📁 [Acesse os Notebooks de ML](./data-science/)
- 🔗 [Link para o Dashboard Interativo](link-do-dash)

---

## 🚀 Como reproduzir este projeto
*(Em construção)*

1. Clone o repositório: git clone https://github.com/lucaspissaia/smia-data-project.git
2. Credenciais do AWS...
3. ...

---

## 👨‍💻 Sobre o Projeto

**Érika Tomoko**, **Juliana Neves**, **Lucas Pissaia**, **Matheus Raeski**

A base deste projeto foi desenvolvida em grupo, como projeto do desafio Discovery, da trilha do Google Cloud, proposto pela FIAP no ano de 2025.
Fomos o campeão da trilha do google, terminamos em Top 2 de todos os projetos apresentados no desafio Discovery.
Agora, após o término do desafio, estou desenvolvendo a parte prática em formato de um MVP (mínimo produto viável), sabendo que posso contar com o auxílio dos meus colegas que ajudaram a conquistar o top 2.

Há uma mudança clara de arquitetura. Mesmo o projeto inicialmente tenso sido pensado em volta da arquitetura da google cloud, o mvp vai ser feito todo em aws - databricks, buscando maior desenvolvimento técnico do autor do mvp.

## 👨‍💻 Sobre o autor deste MVP

Com uma trajetória profissional focada em eficiência, incluindo a experiência atuando como Data & Lean Specialist (Data & Analytics engineer com um adicional de projetos de produtividade) na PepsiCo em Portugal, meu objetivo é sempre garantir que os dados não sejam apenas números em um banco, mas sim engrenagens que geram resultados concretos e otimização na tomada de decisão de ponta a ponta.

🔗 [Conecte-se comigo no LinkedIn]
https://www.linkedin.com/in/lucas-huber-pissaia/
