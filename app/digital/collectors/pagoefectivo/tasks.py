from app.core.celery_app import celery_app
from app.digital.collectors.pagoefectivo.main import get_main_pagoefectivo, get_updated_pagoefectivo
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="pagoefectivo.process_etl")
def task_process_pagoefectivo(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea PagoEfectivo (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_pagoefectivo(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea PagoEfectivo: {e}")
        raise e
    finally:
        release_lock("pagoefectivo-process")

@celery_app.task(bind=True, name="pagoefectivo.process_update")
def task_process_updated_pagoefectivo(self):
    print(f"[WORKER] Iniciando tarea PagoEfectivo Update (ID: {self.request.id})")
    try:
        result = get_updated_pagoefectivo()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea PagoEfectivo Update: {e}")
        raise e
    finally:
        release_lock("pagoefectivo-process")
