from app.digital.collectors.yape.tasks import task_process_yape, task_process_updated_yape
from app.common.constants import (
    TASK_RESULT_TIMEOUT,
    MSG_YAPE_SUCCESS,
    MSG_YAPE_ETL_SUCCESS,
    MSG_TASK_FAILED,
    MSG_TASK_TIMEOUT,
    TASK_STATE_SUCCESS,
    TASK_STATE_FAILURE,
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
    
    
@router.get("/execute-getyape")
@redis_lock("yape-process")
def execute_get_yape(from_date: str = None, to_date: str = None):
    try:
        # Enviar tarea a la cola de Celery
        task = task_process_yape.delay(from_date, to_date)
        
        logger.info(LOG_TASK_STARTED.format(
            task_name="task_process_yape",
            task_id=task.id
        ))
        
        # Esperar el resultado con timeout de 4 horas
        try:
            result = task.get(timeout=TASK_RESULT_TIMEOUT)
            
            # --- VALIDACION DEL RESULTADO DEL WORKER ---
            if not result.get("success", False):
                error_msg = result.get("message", MSG_TASK_FAILED)
                failed_ops = result.get("failed_operations", [])
                success_ops = result.get("successful_operations", [])
                
                # Enriquecer el mensaje para el frontend si hay detalles
                detail_msg = error_msg
                if failed_ops:
                    detail_msg += f"\n\nOperaciones fallidas:\n• " + "\n• ".join(failed_ops)
                if success_ops:
                    detail_msg += f"\n\nOperaciones exitosas:\n• " + "\n• ".join(success_ops)
                
                logger.error(LOG_TASK_FAILURE.format(
                    task_id=task.id,
                    error=detail_msg
                ))
                
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "status": "error",
                        "message": detail_msg,
                        "task_id": task.id,
                        "failed_operations": failed_ops,
                        "successful_operations": success_ops,
                        "result": result
                    }
                )
            # ---------------------------------------------
            
            logger.info(LOG_TASK_SUCCESS.format(task_id=task.id))
            
            # Retornar resultado exitoso
            return {
                "status": "success",
                "message": MSG_YAPE_ETL_SUCCESS,
                "task_id": task.id,
                "result": result,
                "data": {
                    "from_date": from_date,
                    "to_date": to_date
                }
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
            # Re-lanzar excepciones HTTP internas
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
        # Re-lanzar excepciones HTTP ya manejadas
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
        
        
@router.get("/execute-updated-yape")
@redis_lock("yape-process")
def execute_updated_yape():
    try:
        # Enviar tarea a la cola de Celery
        task = task_process_updated_yape.delay()
        
        logger.info(LOG_TASK_STARTED.format(
            task_name="task_process_updated_yape",
            task_id=task.id
        ))
        
        # Esperar el resultado con timeout de 4 horas
        try:
            result = task.get(timeout=TASK_RESULT_TIMEOUT)
            
            # --- VALIDACION DEL RESULTADO DEL WORKER ---
            if not result.get("success", False):
                error_msg = result.get("message", MSG_TASK_FAILED)
                failed_ops = result.get("failed_operations", [])
                success_ops = result.get("successful_operations", [])
                
                # Enriquecer el mensaje para el frontend si hay detalles
                detail_msg = error_msg
                if failed_ops:
                    detail_msg += f"\n\nOperaciones fallidas:\n• " + "\n• ".join(failed_ops)
                if success_ops:
                    detail_msg += f"\n\nOperaciones exitosas:\n• " + "\n• ".join(success_ops)
                
                logger.error(LOG_TASK_FAILURE.format(
                    task_id=task.id,
                    error=detail_msg
                ))
                
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "status": "error",
                        "message": detail_msg,
                        "task_id": task.id,
                        "failed_operations": failed_ops,
                        "result": result
                    }
                )
            # ---------------------------------------------
            
            logger.info(LOG_TASK_SUCCESS.format(task_id=task.id))
            
            # Retornar resultado exitoso
            return {
                "status": "success",
                "message": MSG_YAPE_SUCCESS,
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
            # Re-lanzar excepciones HTTP internas
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
        # Re-lanzar excepciones HTTP ya manejadas
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
