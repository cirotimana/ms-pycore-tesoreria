from app.core.celery_app import celery_app
from app.digital.collectors.kashio.liquidations.main import get_main_kashio_liq
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="kashio.process_liquidations")
def task_process_kashio_liq(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Kashio Liquidations (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_kashio_liq(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Kashio Liquidations: {e}")
        raise e

