from app.digital.collectors.nuvei.tasks import task_process_nuvei, task_process_updated_nuvei
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

@router.get("/execute-getnuvei")
@redis_lock("nuvei-process")
def execute_get_nuvei(from_date: str = None, to_date: str = None):
    try:
        # Aunque el main es async, la tarea de Celery se lanza de forma síncrona aquí (.delay)
        task = task_process_nuvei.delay(from_date, to_date)
        logger.info(LOG_TASK_STARTED.format(task_name="task_process_nuvei", task_id=task.id))
        
        try:
            result = task.get(timeout=TASK_RESULT_TIMEOUT)
            if not result.get("success", False):
                error_msg = result.get("message", MSG_TASK_FAILED)
                failed_ops = result.get("failed_operations", [])
                success_ops = result.get("successful_operations", [])
                detail_msg = f"{error_msg}.\n\nOperaciones fallidas:\n• " + "\n• ".join(failed_ops) if failed_ops else error_msg
                
                logger.error(LOG_TASK_FAILURE.format(task_id=task.id, error=detail_msg))
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
            
            logger.info(LOG_TASK_SUCCESS.format(task_id=task.id))
            return {
                "status": "success",
                "message": result.get("message", "Tarea completada"),
                "task_id": task.id,
                "result": result,
                "data": {"from_date": from_date, "to_date": to_date}
            }
        except CeleryTimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={"status": "timeout", "message": MSG_TASK_TIMEOUT, "task_id": task.id}
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"status": "error", "message": MSG_TASK_FAILED, "task_id": task.id, "error": str(e)}
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "message": f"Error al iniciar la tarea: {str(e)}"}
        )

@router.get("/execute-updated-nuvei")
@redis_lock("nuvei-process")
def execute_updated_nuvei():
    try:
        task = task_process_updated_nuvei.delay()
        logger.info(LOG_TASK_STARTED.format(task_name="task_process_updated_nuvei", task_id=task.id))
        
        try:
            result = task.get(timeout=TASK_RESULT_TIMEOUT)
            if not result.get("success", False):
                error_msg = result.get("message", MSG_TASK_FAILED)
                failed_ops = result.get("failed_operations", [])
                detail_msg = f"{error_msg}.\n\nOperaciones fallidas:\n• " + "\n• ".join(failed_ops) if failed_ops else error_msg
                
                logger.error(LOG_TASK_FAILURE.format(task_id=task.id, error=detail_msg))
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
            
            logger.info(LOG_TASK_SUCCESS.format(task_id=task.id))
            return {
                "status": "success",
                "message": result.get("message", "Actualización completada"),
                "task_id": task.id,
                "result": result
            }
        except CeleryTimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail={"status": "timeout", "message": MSG_TASK_TIMEOUT, "task_id": task.id}
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"status": "error", "message": MSG_TASK_FAILED, "task_id": task.id, "error": str(e)}
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "message": f"Error al iniciar la tarea: {str(e)}"}
        )
