web: python manage.py runserver
worker: python -m celery -A nepse_analytics.celery:app worker --loglevel=info -P gevent