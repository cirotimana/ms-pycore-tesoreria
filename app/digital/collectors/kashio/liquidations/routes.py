from app.digital.collectors.kashio.liquidations.main import get_main_kashio_liq
from fastapi import APIRouter, HTTPException, status
from app.utils.endpoint_lock import endpoint_lock 

router = APIRouter()
    
@router.get("/execute-liqkashio")
@endpoint_lock("kashio-process-liq")
def execute_liq_kashio(from_date = None, to_date = None):
    try:
        result = get_main_kashio_liq(from_date, to_date)
        if result["success"]:
            return {
                "status": "success",
                "message": result["message"],
                "data": {
                    "from_date": from_date,
                    "to_date": to_date,
                    "failed_operations": result["failed_operations"],
                    "successful_operations": result.get("successful_operations", [])
                }
            }
        else:
            error_message = result["message"]
            failed_ops = result["failed_operations"]
            
            detailed_message = f"{error_message}.\n\nOperaciones fallidas:\n• " + "\n• ".join(failed_ops)
            
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "status": "error", 
                    "message": detailed_message, 
                    "failed_operations": failed_ops,  
                    "successful_operations": result.get("successful_operations", []),
                    "data": {
                        "from_date": from_date,
                        "to_date": to_date
                    }
                }
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status": "error",
                "message": f"Error interno en el proceso:\n{str(e)}",
                "failed_operations": ["Error interno del sistema"],
                "successful_operations": [],
                "data": None
            }
        )
