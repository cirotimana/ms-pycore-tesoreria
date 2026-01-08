from fastapi import APIRouter, HTTPException, status
from app.utils.endpoint_lock import lock_manager
from typing import Optional

router = APIRouter()

@router.get("/endpoints-status")
def get_all_endpoints_status():
    status_data = {}
    
    for endpoint_name, lock in lock_manager._locks.items():
        is_running = lock.locked()
        execution_info = lock_manager.get_execution_info(endpoint_name)
        
        status_data[endpoint_name] = {
            "is_running": is_running,
            "execution_info": execution_info if is_running else None
        }
    
    return {
        "status": "success",
        "total_endpoints": len(status_data),
        "running_endpoints": sum(1 for v in status_data.values() if v["is_running"]),
        "endpoints": status_data
    }

@router.get("/endpoints-status/{endpoint_name}")
def get_endpoint_status(endpoint_name: str):
    is_running = lock_manager.is_locked(endpoint_name)
    execution_info = lock_manager.get_execution_info(endpoint_name)
    
    return {
        "status": "success",
        "endpoint": endpoint_name,
        "is_running": is_running,
        "execution_info": execution_info if is_running else None,
        "message": f"El endpoint esta {'en ejecucion' if is_running else 'disponible'}"
    }

@router.get("/endpoints-running")
def get_running_endpoints():
    running = {}
    
    for endpoint_name, lock in lock_manager._locks.items():
        if lock.locked():
            execution_info = lock_manager.get_execution_info(endpoint_name)
            running[endpoint_name] = execution_info
    
    return {
        "status": "success",
        "running_count": len(running),
        "running_endpoints": running
    }

