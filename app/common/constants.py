# ============================================================================
# timeouts y configuraciones
# ============================================================================

# timeout para tareas de celery (4 horas en segundos)
CELERY_TASK_TIMEOUT = 14400  # 4 horas

# timeout para esperar resultados de tareas (4 horas en segundos)
TASK_RESULT_TIMEOUT = 14400  # 4 horas

# tiempo de espera entre reintentos (en segundos)
RETRY_DELAY = 5


# ============================================================================
# mensajes de respuesta - exito
# ============================================================================

MSG_TASK_STARTED = "tarea iniciada en segundo plano"
MSG_TASK_SUCCESS = "tarea completada exitosamente"
MSG_TASK_PROCESSING = "tarea en proceso"

# mensajes especificos por colector
MSG_YAPE_STARTED = "actualizacion yape iniciada en segundo plano"
MSG_YAPE_SUCCESS = "actualizacion yape completada exitosamente"
MSG_YAPE_ETL_STARTED = "proceso yape iniciado en segundo plano"
MSG_YAPE_ETL_SUCCESS = "proceso yape completado exitosamente"


# ============================================================================
# mensajes de respuesta - error
# ============================================================================

MSG_TASK_FAILED = "La tarea fallo durante la ejecucion"
MSG_TASK_TIMEOUT = "la tarea excedio el tiempo maximo de espera"
MSG_TASK_NOT_FOUND = "no se encontro la tarea especificada"
MSG_TASK_PENDING = "la tarea aun esta pendiente de ejecucion"


# ============================================================================
# estados de tareas celery
# ============================================================================

TASK_STATE_PENDING = "PENDING"
TASK_STATE_STARTED = "STARTED"
TASK_STATE_SUCCESS = "SUCCESS"
TASK_STATE_FAILURE = "FAILURE"
TASK_STATE_RETRY = "RETRY"
TASK_STATE_REVOKED = "REVOKED"


# ============================================================================
# codigos de respuesta http
# ============================================================================

HTTP_OK = 200
HTTP_ACCEPTED = 202
HTTP_BAD_REQUEST = 400
HTTP_NOT_FOUND = 404
HTTP_TIMEOUT = 408
HTTP_INTERNAL_ERROR = 500


# ============================================================================
# mensajes de log
# ============================================================================

LOG_TASK_STARTED = "[task_started] tarea {task_name} iniciada con id: {task_id}"
LOG_TASK_SUCCESS = "[task_success] tarea {task_id} completada exitosamente"
LOG_TASK_FAILURE = "[task_failure] tarea {task_id} fallo: {error}"
LOG_TASK_TIMEOUT = "[task_timeout] tarea {task_id} excedio timeout de {timeout}s"
