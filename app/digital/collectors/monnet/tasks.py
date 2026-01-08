from app.core.celery_app import celery_app
from app.digital.collectors.monnet.main import get_main_monnet, get_updated_monnet
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="monnet.process_etl")
def task_process_monnet(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Monnet (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_monnet(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Monnet: {e}")
        raise e
    finally:
        release_lock("monnet-process")

@celery_app.task(bind=True, name="monnet.process_update")
def task_process_updated_monnet(self):
    print(f"[WORKER] Iniciando tarea Monnet Update (ID: {self.request.id})")
    try:
        result = get_updated_monnet()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Monnet Update: {e}")
        raise e
    finally:
        release_lock("monnet-process")
