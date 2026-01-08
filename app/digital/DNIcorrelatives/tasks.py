from app.core.celery_app import celery_app
from app.digital.DNIcorrelatives.main import get_main
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="dni_correlatives.process")
def task_process_dni_correlatives(self):
    print(f"[WORKER] Iniciando tarea DNIcorrelatives (ID: {self.request.id})")
    
    try:
        # Llamada a la logica original sincrona
        result = get_main()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea DNIcorrelatives: {e}")
        raise e
    finally:
        # Liberar el lock de Redis al terminar
        release_lock("dni-correlatives-process")
