from app.core.celery_app import celery_app
from app.digital.collectors.niubiz.main import get_main_niubiz, get_updated_niubiz
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="niubiz.process_etl")
def task_process_niubiz(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Niubiz (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_niubiz(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Niubiz: {e}")
        raise e

@celery_app.task(bind=True, name="niubiz.process_update")
def task_process_updated_niubiz(self):
    print(f"[WORKER] Iniciando tarea Niubiz Update (ID: {self.request.id})")
    try:
        result = get_updated_niubiz()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Niubiz Update: {e}")
        raise e
