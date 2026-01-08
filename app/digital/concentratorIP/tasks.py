from app.core.celery_app import celery_app
from app.digital.concentratorIP.main import get_main
from app.common.redis_lock import release_lock

@celery_app.task(bind=True, name="concentrator_ip.process")
def task_process_concentrator_ip(self):
    print(f"[WORKER] Iniciando tarea ConcentratorIP (ID: {self.request.id})")
    
    try:
        # Llamada a la logica original sincrona
        result = get_main()
        return result
    except Exception as e:
        print(f"[WORKER] Error en tarea ConcentratorIP: {e}")
        raise e
    finally:
        # Liberar el lock de Redis al terminar
        release_lock("concentrator-ip-process")
