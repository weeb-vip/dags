# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow DAGs repository containing workflow definitions for data processing pipelines. The project uses Docker for containerization with Apache Airflow 2.10.5 and Python 3.9.

## Repository Structure

- `dags/` - Contains Airflow DAG definitions (Python files)
- `Dockerfile` - Airflow container configuration with clickhouse-connect dependency
- `README.md` - Basic project documentation

## Development Commands

### Docker Operations
```bash
# Build the Airflow container
docker build -t airflow-dags .

# Run Airflow locally (requires full Airflow setup)
docker run airflow-dags
```

### DAG Development
- Place new DAG files in the `dags/` directory
- Follow the existing pattern in `first_dag.py` for basic DAG structure
- All DAG files must be valid Python modules that define Airflow DAGs

## Architecture

The project follows standard Airflow conventions:
- DAG definitions use the `with DAG()` context manager pattern
- Tasks are defined using Airflow operators (PythonOperator, etc.)
- DAGs include metadata like `dag_id`, `start_date`, `schedule`, and `tags`

## Dependencies

- Apache Airflow 2.10.5
- clickhouse-connect (for ClickHouse database connectivity)
- Python 3.9 base environment

## DAG Patterns

When creating new DAGs:
1. Import required Airflow modules at the top
2. Define task functions before the DAG definition
3. Use descriptive `dag_id` and `task_id` values
4. Set appropriate `start_date`, `schedule`, and `catchup` parameters
5. Add relevant tags for organization