from app.core.celery_app import celery_app
from app.digital.collectors.yape.main import get_main_yape, get_updated_yape
from app.common.redis_lock import release_lock

# Definimos la tarea de Celery
# bind=True nos permite acceder a 'self' y al ID de la tarea
@celery_app.task(bind=True, name="yape.process_etl")
def task_process_yape(self, from_date, to_date):
    print(f"[WORKER] Iniciando tarea Yape (ID: {self.request.id}) para rango: {from_date} - {to_date}")
    
    try:
        # Llamada a tu logica original sincrona
        result = get_main_yape(from_date, to_date)
        return result
    except Exception as e:
        # En caso de error critico, podemos reportarlo
        print(f"[WORKER] Error en tarea Yape: {e}")
        # Re-raise para que Celery marque la tarea como FAILED
        raise e


@celery_app.task(bind=True, name="yape.process_update")
def task_process_updated_yape(self):
    print(f"[WORKER] Iniciando tarea Yape Update (ID: {self.request.id})")
    try:
        result = get_updated_yape()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea Yape Update: {e}")
        raise e

