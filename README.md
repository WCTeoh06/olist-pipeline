# Olist E-Commerce Data Pipeline

A learning project to build a data pipeline with Apache Spark, PostgreSQL and Airflow
using the Olist Brazilian E-Commerce dataset from Kaggle.

## Goal
CSV files -> Spark (clean and transform) -> Parquet / PostgreSQL -> scheduled with Airflow

## Progress
- [x] Environment: WSL2 Ubuntu, Docker, PySpark
- [x] Explore the customers table
- [ ] Clean and join all tables
- [ ] Load into PostgreSQL with a star schema
- [ ] Schedule with Airflow
- [ ] Data quality checks

## Data
Dataset from Kaggle (not included in this repo). Download it into `data/raw/`.
