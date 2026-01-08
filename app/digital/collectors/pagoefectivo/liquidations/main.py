from app.digital.collectors.pagoefectivo.liquidations.analysis import *
from datetime import datetime, timedelta
import pytz

def get_main_pagoefectivo_liq(from_date = None, to_date = None):
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    if not from_date or not to_date:
        from_date = now - timedelta(days=7)
        to_date = from_date
    else:
        fmt = '%d%m%Y' if len(from_date) == 8 else '%d%m%y'
        from_date = datetime.strptime(from_date, fmt).replace(tzinfo=lima_tz)
        to_date = datetime.strptime(to_date, fmt).replace(tzinfo=lima_tz)
        
    print(f"[DEBUG] Enviando fechas from_date : {from_date} , to_date : {to_date}")
    
    try:
        results = {
            'pagoefectivo': get_pagoefectivo_liq(from_date, to_date),
        }
        
        print(f"[DEBUG] Resultados: Pagoefectivo={results['pagoefectivo']}")
    
        all_success = all(results.values())
    
        if all_success:
            print("Todas las operaciones completadas exitosamente")
            return {
                "success": True,
                "message": "Todas las operaciones completadas exitosamente",
                "failed_operations": []
            }
        else:
            
            failed_operations = []
            if not results['pagoefectivo']:
                failed_operations.append("Pagoefectivo - Error en la descarga de datos")
            
            
            print(f"Operaciones fallidas: {failed_operations}")
            return {
                "success": False,
                "message": "Algunas operaciones fallaron",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }
        
    except Exception as e:
        error_message = f"Error en get_main: {e}"
        print(error_message)
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["Error general en el proceso"],
            "successful_operations": []
        }
    


if __name__ == "__main__":
    get_main_pagoefectivo_liq()
