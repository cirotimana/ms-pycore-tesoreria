from app.core.celery_app import celery_app
from app.digital.collectors.tupay.main import get_main_tupay, get_updated_tupay
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="tupay.process_etl")
def task_process_tupay(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Tupay (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_tupay(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Tupay: {e}")
        raise e


@celery_app.task(bind=True, name="tupay.process_update")
def task_process_updated_tupay(self):
    print(f"[WORKER] Iniciando tarea Tupay Update (ID: {self.request.id})")
    try:
        result = get_updated_tupay()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Tupay Update: {e}")
        raise e

