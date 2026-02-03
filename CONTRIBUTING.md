# Contributing to MediTrack360

Thank you for contributing to the MediTrack360 Data Platform.  
This project is a collaborative, production-style data engineering system and follows strict contribution and ownership guidelines to ensure stability and clarity.

## Project Contribution Philosophy

- Each contributor owns specific layers of the platform
- Changes must be isolated, reviewable, and traceable
- No direct commits to the main branch
- All changes go through Pull Requests


## Ownership Model

### Data Engineer 1 – Infrastructure & Ingestion


### Data Engineer 2 – Transformations & Data Quality


### Data Engineer 3 – Warehouse & Production


## Branching Strategy

- `main` – Stable, production-ready code only
- Feature branches:


feature/ingestion-*
feature/transformations-*
feature/redshift-*


Examples:


feature/ingestion-postgres
feature/silver-admissions
feature/redshift-copy



## Pull Request Rules

- All changes must be made via Pull Requests
- PRs must:
  - Reference the owned component
  - Include a short description of the change
  - Pass CI checks
- At least one team member must review before merge


## Commit Message Guidelines

Use clear, descriptive commit messages:



Add Postgres admissions ingestion DAG
Fix timestamp normalization in silver labs
Create Redshift COPY script for fact_admissions


Avoid vague messages like:


update
fix
changes



## Reporting Issues

If you encounter issues:
- Open a GitHub Issue
- Label it appropriately:
  - `infra`
  - `ingestion`
  - `transformation`
  - `warehouse`
  - `ci-cd`


## Code of Conduct

This project is collaborative and professional.  
Respect ownership boundaries and review feedback constructively.
