from app.core.celery_app import celery_app
from app.digital.collectors.kushki.main import get_main_kushki, get_updated_kushki
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="kushki.process_etl")
def task_process_kushki(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Kushki (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_kushki(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Kushki: {e}")
        raise e


@celery_app.task(bind=True, name="kushki.process_update")
def task_process_updated_kushki(self):
    print(f"[WORKER] Iniciando tarea Kushki Update (ID: {self.request.id})")
    try:
        result = get_updated_kushki()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Kushki Update: {e}")
        raise e

