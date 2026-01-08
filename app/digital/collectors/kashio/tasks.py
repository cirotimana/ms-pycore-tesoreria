from app.core.celery_app import celery_app
from app.digital.collectors.kashio.main import get_main_kashio, get_updated_kashio
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="kashio.process_etl")
def task_process_kashio(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Kashio (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_kashio(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Kashio: {e}")
        raise e
    finally:
        release_lock("kashio-process")

@celery_app.task(bind=True, name="kashio.process_update")
def task_process_updated_kashio(self):
    print(f"[WORKER] Iniciando tarea Kashio Update (ID: {self.request.id})")
    try:
        result = get_updated_kashio()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Kashio Update: {e}")
        raise e
    finally:
        release_lock("kashio-process")
