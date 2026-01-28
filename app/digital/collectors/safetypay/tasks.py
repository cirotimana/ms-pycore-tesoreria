from app.core.celery_app import celery_app
from app.digital.collectors.safetypay.main import get_main_safetypay, get_updated_safetypay
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="safetypay.process_etl")
def task_process_safetypay(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea SafetyPay (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_safetypay(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea SafetyPay: {e}")
        raise e


@celery_app.task(bind=True, name="safetypay.process_update")
def task_process_updated_safetypay(self):
    print(f"[WORKER] Iniciando tarea SafetyPay Update (ID: {self.request.id})")
    try:
        result = get_updated_safetypay()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea SafetyPay Update: {e}")
        raise e

