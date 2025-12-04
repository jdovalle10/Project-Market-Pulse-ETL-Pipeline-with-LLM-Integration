# Project Market Pulse: ETL Pipeline with LLM Integration

## Project Overview
Project Market Pulse demonstrates how to integrate a Large Language Model (LLM), specifically the OpenAI (ChatGPT) API, within a modern Data Engineering ETL pipeline.

The goal is to convert unstructured global news articles into structured market intelligence by generating:

- Sentiment (`sentiment_llm`)
- Category (`category_llm`)
- Market Impact Summary (`market_impact_summary`)

This project covers the full data engineering lifecycle, showing how LLMs can be operationalized within scalable pipelines.

---

## Business Objective
The objective is to validate the practicality and efficiency of LLMs in early detection of market risks and opportunities.

By analyzing political, financial, economic, and global news, the pipeline generates:

1. LLM-generated sentiment  
2. LLM-generated category  
3. Market impact summary  

These insights enable analysts and decision-makers to detect signals earlier and more accurately.

---

## Architecture and Pipeline Stages

### ETL Workflow (main.py)

| Stage | Description | Input | Output |
|-------|-------------|--------|---------|
| **STAGE 1 (clean)** | Extract raw JSONL, clean missing fields, reorder chronologically | Raw `.jsonl` | Clean Parquet in S3 (`/clean/`) |
| **STAGE 2 (enrich)** | Apply LLM enrichment using OpenAI API in controlled batches | Clean Parquet | Enriched Parquet in S3 (`/enriched/`) |
| **STAGE 3 (load)** | Update AWS Glue Catalog with `MSCK REPAIR TABLE` | Enriched Parquet | Athena-queryable table |

---

## Tools and Technologies

| Category | Tools Used |
|----------|------------|
| Orchestration | Apache Airflow (MWAA), AWS Batch |
| Processing | Python, Pandas, argparse |
| LLM / API | OpenAI API (GPT-3.5-Turbo) |
| Data Lake | Amazon S3, Parquet |
| Query / Data Warehouse | Amazon Athena, AWS Glue Catalog |
| Visualization | Power BI |

---

## Project Status and Testing
The project has been fully tested locally:

- Core ETL logic in `main.py` validated  
- Batch mechanism in Stage 2 handles rate limits and retries  
- Output generated in Parquet format  
- Schema compatible with AWS Glue and Athena  

---

## Future Roadmap

### Deployment
- Containerize ETL using Docker  
- Push images to AWS ECR  
- Orchestrate with Airflow DAGs using AWS Batch/ECS  

### Security
- Store OpenAI API keys using AWS Secrets Manager  
- Remove all local plain-text credentials  

### Validation
- Conduct stress tests for API cost, performance, and throughput  
- Optimize Athena query performance  

### Final Delivery
- Connect Power BI to Athena  
- Develop dashboards showing sentiment trends, category distributions, and market impact summaries

# 📚 References

This project utilizes the **News Category Dataset** for its news analysis.

1.  Misra, Rishabh. "News Category Dataset." arXiv preprint arXiv:2209.11429 (2022).
2.  Misra, Rishabh and Jigyasa Grover. "Sculpting Data for ML: The first act of Machine Learning." ISBN 9798585463570 (2021).

---


