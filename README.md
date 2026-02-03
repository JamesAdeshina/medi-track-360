# MediTrack360: Collaborative Healthcare Operations Data Platform

## Project Overview
MediTrack360 is a collaborative, production-grade data platform project simulating hospital operations. It integrates Data Engineering and Data Analytics roles in a realistic healthcare workflow to build a full end-to-end data platform powering operational dashboards for a 300-bed hospital.

## Problem Statement
Hospitals face challenges such as long waiting times, bed shortages, medicine stockouts, disconnected systems, and manual reporting. Management requires a unified analytics platform showing real-time operational performance across admissions, pharmacy, labs, and staff workload.

## Team Roles
- **Data Engineer (DE):** Builds entire data pipeline including infrastructure, ingestion, transformation, data quality, and CI/CD
- **Data Analyst (DA):** Defines KPIs, creates SQL models, builds Power BI dashboards, and provides insights

## Technologies
- **Infrastructure:** Terraform, Docker, AWS (S3, IAM, Redshift)
- **Data Processing:** PostgreSQL, Apache Airflow, PySpark, Kafka
- **Data Quality:** Great Expectations
- **Warehouse:** Amazon Redshift
- **Visualization:** Power BI
- **CI/CD:** GitHub Actions

## Architecture
Data flows through a medallion architecture:
1. **Bronze Layer:** Raw data from PostgreSQL, Pharmacy API, and Lab CSVs
2. **Silver Layer:** Cleaned, standardized Parquet data with validated fields
3. **Gold Layer:** Fact and dimension tables optimized for analytics
4. **Presentation Layer:** Redshift for Power BI consumption

