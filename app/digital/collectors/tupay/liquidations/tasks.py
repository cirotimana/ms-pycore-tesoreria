from app.core.celery_app import celery_app
from app.digital.collectors.tupay.liquidations.main import get_main_tupay_liq
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="tupay.process_liquidations")
def task_process_tupay_liq(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Tupay Liquidations (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    try:
        result = get_main_tupay_liq(from_date, to_date)
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Tupay Liquidations: {e}")
        raise e
    finally:
        release_lock("tupay-process-liq")
