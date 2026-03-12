from app.core.celery_app import celery_app
from app.digital.collectors.monnet.main import get_main_monnet, get_updated_monnet
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="monnet.process_etl")
def task_process_monnet(self, from_date, to_date):
    print(f"[worker] iniciando tarea monnet (id: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_monnet(from_date, to_date)
        return result
    except Exception as e:
        print(f"[worker] error en tarea monnet: {e}")
        raise e


@celery_app.task(bind=True, name="monnet.process_update")
def task_process_updated_monnet(self):
    print(f"[worker] iniciando tarea monnet update (id: {self.request.id})")
    try:
        result = get_updated_monnet()
        return result
    except Exception as e:
        print(f"[worker] error en tarea monnet update: {e}")
        raise e

