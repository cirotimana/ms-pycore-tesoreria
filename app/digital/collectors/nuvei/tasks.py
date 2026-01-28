import asyncio
from app.core.celery_app import celery_app
from app.digital.collectors.nuvei.main import get_main_nuvei, get_updated_nuvei
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="nuvei.process_etl")
def task_process_nuvei(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Nuvei (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        # Nuvei es async, usamos un event loop para ejecutarlo
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(get_main_nuvei(from_date, to_date))
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Nuvei: {e}")
        raise e


@celery_app.task(bind=True, name="nuvei.process_update")
def task_process_updated_nuvei(self):
    print(f"[WORKER] Iniciando tarea Nuvei Update (ID: {self.request.id})")
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(get_updated_nuvei())
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Nuvei Update: {e}")
        raise e

