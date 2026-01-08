import asyncio
from functools import wraps
from typing import Dict, Optional
from datetime import datetime
from fastapi import HTTPException, status

class EndpointLockManager:

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._execution_info: Dict[str, dict] = {}
    
    def get_lock(self, endpoint_name: str) -> asyncio.Lock:
        """Obtiene o crea un lock para un endpoint especifico"""
        if endpoint_name not in self._locks:
            self._locks[endpoint_name] = asyncio.Lock()
        return self._locks[endpoint_name]
    
    def is_locked(self, endpoint_name: str) -> bool:
        """Verifica si un endpoint esta bloqueado (en ejecucion)"""
        lock = self._locks.get(endpoint_name)
        return lock.locked() if lock else False
    
    def set_execution_info(self, endpoint_name: str, info: dict):
        """Guarda informacion sobre la ejecucion actual"""
        self._execution_info[endpoint_name] = {
            **info,
            "started_at": datetime.now().isoformat(),
            "current_endpoint": info.get("current_endpoint", endpoint_name)
        }
    
    def get_execution_info(self, endpoint_name: str) -> Optional[dict]:
        """Obtiene informacion sobre la ejecucion actual"""
        return self._execution_info.get(endpoint_name)
    
    def clear_execution_info(self, endpoint_name: str):
        """Limpia la informacion de ejecucion"""
        if endpoint_name in self._execution_info:
            del self._execution_info[endpoint_name]

# Instancia global del gestor de locks
lock_manager = EndpointLockManager()


def endpoint_lock(endpoint_name: str):
   
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            lock = lock_manager.get_lock(endpoint_name)
            
            # Verificar si el endpoint esta en ejecucion
            if lock.locked():
                execution_info = lock_manager.get_execution_info(endpoint_name)
                current_endpoint = execution_info.get("current_endpoint", endpoint_name) if execution_info else endpoint_name
                
                # Determinar si es el mismo endpoint o uno relacionado
                if current_endpoint == func.__name__:
                    blocking_msg = "El proceso ya se encuentra en ejecucion por otro usuario"
                else:
                    blocking_msg = "No se puede ejecutar el proceso, porque se encuentra en ejecucion por otro usuario"
                
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "status": "locked",
                        "message": blocking_msg,
                        "requested_endpoint": func.__name__,
                        "lock_name": endpoint_name,
                        "blocking_endpoint": current_endpoint,
                        "execution_info": execution_info or {},
                        "suggestion": "Por favor, espere a que la ejecucion actual termine antes de iniciar una nueva."
                    }
                )
            
            # Adquirir el lock
            async with lock:
                # Guardar informacion de la ejecucion
                lock_manager.set_execution_info(endpoint_name, {
                    "endpoint": endpoint_name,
                    "current_endpoint": func.__name__,
                    "parameters": {k: v for k, v in kwargs.items() if v is not None}
                })
                
                try:
                    # Ejecutar la funcion
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    # Limpiar informacion de ejecucion
                    lock_manager.clear_execution_info(endpoint_name)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            lock = lock_manager.get_lock(endpoint_name)
            
            # Verificar si el endpoint esta en ejecucion
            if lock.locked():
                execution_info = lock_manager.get_execution_info(endpoint_name)
                current_endpoint = execution_info.get("current_endpoint", endpoint_name) if execution_info else endpoint_name
                
                # Determinar si es el mismo endpoint o uno relacionado
                if current_endpoint == func.__name__:
                    blocking_msg = "El proceso ya se encuentra en ejecucion por otro usuario"
                else:
                    blocking_msg = "No se puede ejecutar el proceso, porque se encuentra en ejecucion por otro usuario"
                
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "status": "locked",
                        "message": blocking_msg,
                        "requested_endpoint": func.__name__,
                        "lock_name": endpoint_name,
                        "blocking_endpoint": current_endpoint,
                        "execution_info": execution_info or {},
                        "suggestion": "Por favor, espere a que la ejecucion actual termine antes de iniciar una nueva."
                    }
                )
            
            # Simular comportamiento de lock para funciones sincronas
            import threading
            thread_lock = threading.Lock()
            
            with thread_lock:
                # Marcar el asyncio lock como ocupado
                lock._locked = True
                
                # Guardar informacion de la ejecucion
                lock_manager.set_execution_info(endpoint_name, {
                    "endpoint": endpoint_name,
                    "current_endpoint": func.__name__,
                    "parameters": {k: v for k, v in kwargs.items() if v is not None}
                })
                
                try:
                    # Ejecutar la funcion
                    result = func(*args, **kwargs)
                    return result
                finally:
                    # Limpiar informacion de ejecucion
                    lock_manager.clear_execution_info(endpoint_name)
                    lock._locked = False
        
        # Retornar el wrapper apropiado segun si la funcion es async o no
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator
