PYTHON = python3
VENV = .venv
VENV_BIN = $(VENV)/bin
PYTHON_VENV= $(VENV_BIN)/python3

DOCKER_COMPOSE = ./docker/docker-compose.yml
DATA_INPUT_PATH=../../data
TESTS_FOLDER = ./tests

PROJECT_ID=protonx-de-01
BUCKET_NAME=gs://trading-data-bucket-$(PROJECT_ID)

setup_python_env:
	@echo "Create Python venv environment"
	@$(PYTHON) -m venv $(VENV)
	@$(VENV_BIN)/pip install -U pip
	@$(VENV_BIN)/pip install -r requirements.txt
	@echo "Complete"

setup_docker: 
	@docker-compose -f $(DOCKER_COMPOSE) build 
	@docker-compose -f $(DOCKER_COMPOSE) up  -d

docker_up:
	@docker-compose -f $(DOCKER_COMPOSE) up  -d

docker_down:
	@docker-compose -f $(DOCKER_COMPOSE) down

test: 
	@$(PYTHON_VENV) -m pytest $(TESTS_FOLDER)/unit

delete_cached: 
	@rm -rf $(VENV)
	@echo "Deleting __pycache__"
	@find . -type d -name "__pycache__" -exec rm -r {} +        
	@find . -type d -name ".pytest_cache" -exec rm -r {} +        

teardown:
	@docker-compose -f $(DOCKER_COMPOSE) down -v --rmi all

setup: setup_python_env setup_docker

clean: teardown delete_cached
