# CowSwap Data Pipeline

This project is designed to perform daily data processing tasks related to trades on CowSwap. The pipeline is scheduled using Apache Airflow and interacts with various APIs and a PostgreSQL database.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Project Setup](#project-setup)
- [Running the Project](#running-the-project)
- [Airflow Setup](#airflow-setup)
- [Database Setup](#database-setup)
- [Testing](#testing)
- [Linting](#linting)
- [License](#license)

## Requirements

- Python 3.10+
- [Poetry](https://python-poetry.org/docs/#installation) (for dependency management)
- PostgreSQL 13+
- Apache Airflow 2.10+
- GCC and other build tools (for building some Python packages)

## Installation


```bash
cd CowSwap
```

## 1. Install Python and Poetry

Ensure you have Python 3.10+ installed. You can check your Python version by running:

```bash
python3 --version
```
Install Poetry, which will handle dependency management:
```bash
curl -sSL https://install.python-poetry.org | python3 -
or
pip install poetry
poetry shell
poetry install
```
## 2. Install Project Dependencies
Run the following command to install all dependencies, including Airflow:
```bash
make install_deps
```
## 3. Set Up the Configurations Variables

### Dune setup

Go to : https://dune.com/data/cow_protocol_ethereum.trades, Signup, create an **API key** and create a new Query

```bash
WITH latest_day AS (
  SELECT
    MAX(block_date) AS latest_date
  FROM cow_protocol_ethereum.trades
  WHERE
    token_pair = 'USDC-WETH'
)
SELECT
  block_time,
  block_number,
  sell_token_address,
  sell_token,
  buy_token,
  token_pair,
  buy_price,
  sell_price,
  units_sold
FROM cow_protocol_ethereum.trades
WHERE
  token_pair = 'USDC-WETH'
  AND block_date = (
    SELECT
      latest_date
    FROM latest_day
  )
ORDER BY
  block_time
```
Save the query and get the **query_id**

### Configuration

Fill the config.yml with the corresponding API_KEY and QUERY_ID

```bash
db_params:
  dbname: "cow_swap"
  user: "user1"
  password: "password123"
  host: "localhost"
  port: "5432"
  batch_size: 100

dune_api:
  api_key: ""
  query_id: 

currencies:
  currency_1: "weth"
  currency_2: "usdc"
```

## Project Setup

## 1. Initialize the PostgreSQL Database

First, ensure PostgreSQL is installed and running. Then, initialize the database:

```bash
make full_reinit
```
This will Create the Database, schema, user, grants and the 2 tables

## 2. Initialize Airflow
Airflow needs to be initialized to set up its metadata database:
```bash
make init_airflow
```
## Running the Project

## Start Airflow Services
Start the Airflow webserver and scheduler:
```bash
make start_airflow
```
You can access the Airflow web UI at http://localhost:8080.

## Trigger the DAG
Trigger the DAG manually for testing:
```bash
make trigger_dag DAG_ID=daily_data_pipeline
```

## Running Manually
Run the project 
```bash
python3 main.py
```
