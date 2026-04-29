init:
	python -m scripts.init --all

run-server:
	uvicorn api.main:app --host 0.0.0.0 --port 8001

run-worker:
	celery -A worker.celery_app worker --loglevel=info