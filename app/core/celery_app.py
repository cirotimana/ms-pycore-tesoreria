from celery import Celery
import os
from app.config import Config
from app.common.constants import CELERY_TASK_TIMEOUT

# Usar variable de entorno o fallback a localhost para desarrollo local sin docker
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")

celery_app = Celery(
    "worker_app",
    broker=BROKER_URL,
    backend=BROKER_URL
)

celery_app.conf.update(
    task_track_started=True,
    task_time_limit=CELERY_TASK_TIMEOUT,  # 4 horas maximo por tarea (desde constants.py)
    worker_prefetch_multiplier=1, # Un worker toma una tarea a la vez (crucial para Pandas)
    task_acks_late=True,
    broker_connection_retry_on_startup=True
)

# Autodiscovery buscara tareas en los paquetes indicados
celery_app.autodiscover_tasks([
    'app.digital.collectors.yape',
    'app.digital.collectors.kashio',
    'app.digital.collectors.kushki',
    'app.digital.collectors.monnet',
    'app.digital.collectors.niubiz',
    'app.digital.collectors.nuvei',
    'app.digital.collectors.pagoefectivo',
    'app.digital.collectors.safetypay',
    'app.digital.collectors.tupay',
])
