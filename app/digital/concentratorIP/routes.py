from app.digital.concentratorIP.tasks import task_process_concentrator_ip
from app.common.constants import (
    TASK_RESULT_TIMEOUT,
    MSG_TASK_FAILED,
    MSG_TASK_TIMEOUT,
    LOG_TASK_STARTED,
    LOG_TASK_SUCCESS,
    LOG_TASK_FAILURE,
    LOG_TASK_TIMEOUT
)
from app.common.redis_lock import redis_lock
from fastapi import APIRouter, HTTPException, status
from celery.exceptions import TimeoutError as CeleryTimeoutError
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
    
    
@router.get("/execute-concentratorip")
@redis_lock("concentrator-ip-process")
def execute_concentratorip():
    try:
        # Enviar tarea a la cola de Celery
        task = task_process_concentrator_ip.delay()
        
        logger.info(LOG_TASK_STARTED.format(
            task_name="task_process_concentrator_ip",
            task_id=task.id
        ))
        
        # Esperar el resultado
        try:
            result = task.get(timeout=TASK_RESULT_TIMEOUT)
            
            if not result.get("success", False):
                error_msg = result.get("message", MSG_TASK_FAILED)
                
                logger.error(LOG_TASK_FAILURE.format(
                    task_id=task.id,
                    error=error_msg
                ))
                
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "status": "error",
                        "message": error_msg,
                        "task_id": task.id,
                        "result": result
                    }
                )
            
            logger.info(LOG_TASK_SUCCESS.format(task_id=task.id))
            
            return {
                "status": "success",
                "message": result.get("message", "Correo enviado con exito"),
                "task_id": task.id,
                "result": result
            }
            
        except CeleryTimeoutError:
            logger.warning(LOG_TASK_TIMEOUT.format(
                task_id=task.id,
                timeout=TASK_RESULT_TIMEOUT
            ))
            
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={
                    "status": "timeout",
                    "message": MSG_TASK_TIMEOUT,
                    "task_id": task.id,
                    "timeout_seconds": TASK_RESULT_TIMEOUT
                }
            )
            
        except HTTPException:
            raise
            
        except Exception as task_error:
            logger.error(LOG_TASK_FAILURE.format(
                task_id=task.id,
                error=str(task_error)
            ))
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "status": "error",
                    "message": MSG_TASK_FAILED,
                    "task_id": task.id,
                    "error": str(task_error)
                }
            )

    except HTTPException:
        raise
        
    except Exception as e:
        logger.error(f"Error al iniciar tarea: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status": "error",
                "message": f"Error al iniciar la tarea: {str(e)}"
            }
        )