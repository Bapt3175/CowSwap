# Install all dependencies including Poetry, Airflow, and others from pyproject.toml
install_deps:
	@echo "Installing dependencies..."
	@python3 -m pip install psycopg2-binary
	@python3 -m pip install --upgrade pip poetry apache-airflow
	@poetry install

# Initialize Airflow and set up the admin user
init_airflow:
	@echo "Initializing Airflow..."
	@airflow db init
	@airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || echo "User 'admin' already exists."

# Start Airflow services
start_airflow:
	@echo "Starting Airflow webserver and scheduler..."
	@airflow webserver --port 8080 &
	@airflow scheduler &

# Trigger a specific DAG
trigger_dag:
	@echo "Triggering the DAG $(DAG_ID)..."
	@airflow dags trigger $(DAG_ID)

# Stop all Airflow services
stop_airflow:
	@echo "Stopping Airflow services..."
	@pkill -f "airflow webserver" || true
	@pkill -f "airflow scheduler" || true

# Combined command to initialize Airflow, start services, and trigger the DAG
run_pipeline: install_deps init_airflow start_airflow
	@echo "Waiting for Airflow to fully start..."
	@sleep 15  # Give some time for Airflow to fully start
	@make trigger_dag DAG_ID=$(DAG_ID)

# Lint the codebase with Ruff
lint:
	@poetry run ruff check .

# Run tests with coverage
test:
	@poetry run coverage run -m pytest
	@poetry run coverage report -m
	@poetry run coverage html
	@echo "Coverage report generated: open htmlcov/index.html to view"

# Install, lint, and test
all: install_deps lint test

# PostgreSQL operations
install_postgres:
	@sudo apt-get update && sudo apt-get install -y postgresql postgresql-contrib python3-psycopg2

start_postgres:
	@echo "Starting PostgreSQL service..."
	@sudo service postgresql start

# Stop PostgreSQL service
stop_postgres:
	@echo "Stopping PostgreSQL service..."
	@sudo service postgresql stop

# Start PostgreSQL service
start_postgres:
	@echo "Starting PostgreSQL service..."
	@sudo service postgresql start

# Read database configuration from config.yml
read_db_config:
	@echo "Reading database configuration from config.yml..."
	$(eval DBNAME=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['dbname'])"))
	$(eval USER=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['user'])"))
	$(eval PASSWORD=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['password'])"))
	$(eval HOST=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['host'])"))
	$(eval PORT=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['port'])"))
	$(eval BATCH_SIZE=$(shell python3 -c "import yaml; config = yaml.safe_load(open('config.yml', 'r')); print(config['db_params']['batch_size'])"))
	@echo "Database configuration loaded: DBNAME=$(DBNAME), USER=$(USER), HOST=$(HOST), PORT=$(PORT), BATCH_SIZE=$(BATCH_SIZE)"

init_db: install_postgres start_postgres
	@sudo -u postgres psql -c "CREATE DATABASE $(DBNAME);" || echo "Database $(DBNAME) already exists."
	@sudo -u postgres psql -c "CREATE USER $(USER) WITH PASSWORD '$(PASSWORD)';" || echo "User $(USER) already exists."
	@sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $(DBNAME) TO $(USER);"

create_table: read_db_config
	@python3 -c 'from cow_swap.database.db_provider import PostgreSQLProvider; from yaml import safe_load; \
                 config = safe_load(open("config.yml", "r")); db_params = config["db_params"]; \
                 provider = PostgreSQLProvider(**db_params); \
                 provider.create_table_cow_swap_if_not_exists(); provider.create_table_for_average_improvement()'

clean_db: start_postgres read_db_config
	@sudo -u postgres psql -c "DROP DATABASE IF EXISTS $(DBNAME);" && sudo -u postgres psql -c "DROP USER IF EXISTS $(USER);"

# Combined initialization
init: init_db create_table

full_reinit: stop_postgres clean_db init

# Help
help:
	@echo "Makefile commands:"
	@echo "  make install_deps      - Install all dependencies including Airflow and project dependencies"
	@echo "  make init_airflow      - Initialize Airflow database and create admin user"
	@echo "  make start_airflow     - Start the Airflow webserver"
	@echo "  make start_scheduler   - Start the Airflow scheduler"
	@echo "  make stop_airflow      - Stop all Airflow services"
	@echo "  make run_pipeline      - Initialize Airflow, start services, and trigger a DAG"
	@echo "  make lint              - Run ruff for code linting"
	@echo "  make test              - Run tests with coverage"
	@echo "  make all               - Install dependencies, lint code, and run tests"
	@echo "  make install_postgres  - Install PostgreSQL and required packages"
	@echo "  make start_postgres    - Start the PostgreSQL service"
	@echo "  make stop_postgres     - Stop the PostgreSQL service"
	@echo "  make init_db           - Initialize the PostgreSQL database and user"
	@echo "  make create_table      - Create necessary tables if they do not exist"
	@echo "  make clean_db          - Drop the PostgreSQL database and user"
	@echo "  make init              - Full initialization process (init_db, create_table)"
	@echo "  make full_reinit       - Full reinitialization (drop, init, create tables)"
