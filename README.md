# 🦟 SMIA: Sistema de Mapeamento Inteligente de Arboviroses
**Transformando a gestão de saúde pública de reativa para preditiva com Google Cloud.**

![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-green)
![GCP](https://img.shields.io/badge/Google_Cloud-Native-blue)
![Top2](https://img.shields.io/badge/Award-Top_2_FIAP_Discovery-gold)

---

## 📌 O Problema e o Impacto de Negócio
Atualmente, o Brasil gasta bilhões anualmente com ações reativas contra arboviroses (Dengue, Zika, Chikungunya). O sistema oficial de saúde funciona como um "espelho retrovisor": a notificação ocorre dias após o contágio, quando o surto já está em andamento.

**O objetivo do SMIA é atuar como um "GPS":** cruzar dados históricos de infecção com dados climáticos para prever surtos com semanas de antecedência, permitindo que a gestão pública atue de forma preventiva e otimize o uso de verbas.

**Escopo do projeto atual:** como um MVP (minimum Viable Product) focaremos em Dengue para a cidade de São Paulo.

---

## 🏗️ Arquitetura da Solução (Data de Ponta a Ponta)
`![Diagrama de Arquitetura](C:\Users\Lucas\Documents\smia-data-project\docs\Diagrama de Arquitetura.png)`

O projeto foi estruturado em três pilares principais, refletindo o ciclo de vida completo do dado:

### 1. ⚙️ Engenharia de Dados (Extração e Data Lake)
Responsável por orquestrar a coleta diária e automatizada de dados brutos.
- **Fontes:** API INMET (Clima) e SINAN/DataSUS (Saúde).
- **Ferramentas:** Python, Google Cloud Functions, Cloud Scheduler.
- **Destino:** Google Cloud Storage (Camada Bronze).
- 📁 [Acesse os códigos de Engenharia](./data-engineering/)

### 2. 📊 Analytics Engineering (Modelagem e DW)
Transformação dos dados brutos em um modelo dimensional otimizado para análises e machine learning.
- **Ferramentas:** dbt (Data Build Tool), BigQuery, SQL.
- **Camadas:** Silver (Limpeza e Padronização) e Gold (Tabelas Fato e Dimensão).
- 📁 [Acesse os modelos do dbt](./analytics-engineering/)

### 3. 🧠 Data Science & Analytics (Predição e Consumo)
Geração de insights de negócio e modelagem preditiva de surtos.
- **Ferramentas:** Looker Studio (Dashboard B2G), Python, Vertex AI (Machine Learning).
- 📁 [Acesse os Notebooks de ML](./data-science/)
- 🔗 [Link para o Dashboard Interativo (Looker)](link-do-looker)

---

## 🚀 Como reproduzir este projeto
*(Aqui você colocará um passo a passo simples de como alguém pode rodar o seu código localmente, quais variáveis de ambiente configurar, etc. Deixe para preencher por último)*

1. Clone o repositório: `git clone https://github.com/seu-usuario/smia-data-project.git`
2. Configure as credenciais do GCP...
3. ...

---

## 👨‍💻 Sobre o Autor

**Lucas Pissaia**
Desenvolvi a base deste projeto como trabalho de conclusão do MBA em Data Science & AI pela FIAP, onde fomos reconhecidos no Top 2 do desafio Discovery (trilha Google). 

Com uma trajetória profissional focada em eficiência, incluindo a experiência atuando como Data & Lean Specialist na PepsiCo em Portugal, meu objetivo é sempre garantir que os dados não sejam apenas números em um banco, mas sim engrenagens que geram resultados concretos e otimização na tomada de decisão de ponta a ponta.

🔗 [Conecte-se comigo no LinkedIn](link-do-seu-linkedin)