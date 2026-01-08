import redis
import os
import time
from functools import wraps
from fastapi import HTTPException, status
from app.common.constants import LOG_TASK_FAILURE

# Configuracion de Redis
REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
redis_client = redis.from_url(REDIS_URL)

def redis_lock(lock_name_prefix: str, expire: int = 14400):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            lock_key = f"lock:{lock_name_prefix}"
            
            is_locked = redis_client.set(lock_key, "locked", ex=expire, nx=True)

            collector = lock_name_prefix.split('-')[0].capitalize()
            
            if not is_locked:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "status": "locked",
                        "message": f"El proceso para {collector} ya se encuentra en ejecucion o en cola.",
                        "suggestion": "Por favor, espere a que la ejecucion actual termine."
                    }
                )
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                redis_client.delete(lock_key)
                raise e
        return wrapper
    return decorator

def release_lock(lock_name_prefix: str):
    lock_key = f"lock:{lock_name_prefix}"
    redis_client.delete(lock_key)
