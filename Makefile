up:
	docker-compose up -d

down:
	docker-compose down

export-env:
	export $(cat .env | xargs)

export-python:
	export PYTHONPATH=$PYTHONPATH:$(pwd)