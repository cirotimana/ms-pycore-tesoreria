# ============================================================================
# TIMEOUTS Y CONFIGURACIONES
# ============================================================================

# Timeout para tareas de Celery (4 horas en segundos)
CELERY_TASK_TIMEOUT = 14400  # 4 horas

# Timeout para esperar resultados de tareas (4 horas en segundos)
TASK_RESULT_TIMEOUT = 14400  # 4 horas

# Tiempo de espera entre reintentos (en segundos)
RETRY_DELAY = 5


# ============================================================================
# MENSAJES DE RESPUESTA - eXITO
# ============================================================================

MSG_TASK_STARTED = "Tarea iniciada en segundo plano"
MSG_TASK_SUCCESS = "Tarea completada exitosamente"
MSG_TASK_PROCESSING = "Tarea en proceso"

# Mensajes especificos por colector
MSG_YAPE_STARTED = "Actualizacion Yape iniciada en segundo plano"
MSG_YAPE_SUCCESS = "Actualizacion Yape completada exitosamente"
MSG_YAPE_ETL_STARTED = "Proceso Yape iniciado en segundo plano"
MSG_YAPE_ETL_SUCCESS = "Proceso Yape completado exitosamente"


# ============================================================================
# MENSAJES DE RESPUESTA - ERROR
# ============================================================================

MSG_TASK_FAILED = "La tarea fallo durante la ejecucion"
MSG_TASK_TIMEOUT = "La tarea excedio el tiempo maximo de espera"
MSG_TASK_NOT_FOUND = "No se encontro la tarea especificada"
MSG_TASK_PENDING = "La tarea aun esta pendiente de ejecucion"


# ============================================================================
# ESTADOS DE TAREAS CELERY
# ============================================================================

TASK_STATE_PENDING = "PENDING"
TASK_STATE_STARTED = "STARTED"
TASK_STATE_SUCCESS = "SUCCESS"
TASK_STATE_FAILURE = "FAILURE"
TASK_STATE_RETRY = "RETRY"
TASK_STATE_REVOKED = "REVOKED"


# ============================================================================
# CoDIGOS DE RESPUESTA HTTP
# ============================================================================

HTTP_OK = 200
HTTP_ACCEPTED = 202
HTTP_BAD_REQUEST = 400
HTTP_NOT_FOUND = 404
HTTP_TIMEOUT = 408
HTTP_INTERNAL_ERROR = 500


# ============================================================================
# MENSAJES DE LOG
# ============================================================================

LOG_TASK_STARTED = "[TASK_STARTED] Tarea {task_name} iniciada con ID: {task_id}"
LOG_TASK_SUCCESS = "[TASK_SUCCESS] Tarea {task_id} completada exitosamente"
LOG_TASK_FAILURE = "[TASK_FAILURE] Tarea {task_id} fallo: {error}"
LOG_TASK_TIMEOUT = "[TASK_TIMEOUT] Tarea {task_id} excedio timeout de {timeout}s"
