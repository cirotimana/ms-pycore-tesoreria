from app.digital.collectors.kashio.analysis import *
from datetime import datetime, timedelta
import pytz
import time
from app.common.utils import validate_date_range

def get_main_kashio(from_date = None, to_date = None):
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    if not from_date or not to_date:
        from_date = now - timedelta(days=1)
        to_date = from_date
    else:
        fmt = '%d%m%Y' if len(from_date) == 8 else '%d%m%y'
        from_date = datetime.strptime(from_date, fmt).replace(tzinfo=lima_tz)
        to_date = datetime.strptime(to_date, fmt).replace(tzinfo=lima_tz)
        
    print(f"[DEBUG] Enviando fechas from_date : {from_date} , to_date : {to_date}")

    # validar rango maximo de 10 dias (conteo inclusivo)
    valid, from_date, to_date = validate_date_range(from_date, to_date)
    if not valid:
        return {"success": False, "message": "rango o formato invalido"}

    start_time = time.time()
    try:       
        results = {
            'kashio': get_data_kashio(from_date, to_date),
            'calimaco': get_data_calimaco(from_date, to_date)
        }
        
        if results['kashio'] and results['calimaco']:
            results['conciliation'] = conciliation_data(from_date, to_date)
        else:
            results['conciliation'] = False
        
        print(f"[debug] resultados: kashio={results['kashio']}, calimaco={results['calimaco']}, conciliacion={results['conciliation']}")
    
        all_success = all(results.values())
        elapsed_time = time.time() - start_time
    
        if all_success:
            print(f"todas las operaciones completadas exitosamente en {elapsed_time:.2f} segundos")
            return {
                "success": True,
                "message": f"todas las operaciones completadas exitosamente en {elapsed_time:.2f} segundos",
                "failed_operations": []
            }
        else:
            
            failed_operations = []
            if not results['kashio']:
                failed_operations.append("kashio - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results.get('conciliation'):
                failed_operations.append("conciliacion - no se pudo realizar la conciliacion")
            
            print(f"operaciones fallidas despues de {elapsed_time:.2f} segundos: {failed_operations}")
            return {
                "success": False,
                "message": "algunas operaciones fallaron",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }
        
    except Exception as e:
        error_message = f"error en get_main: {e}"
        print(error_message)
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }


def get_updated_kashio():
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    
    print(f"[debug] enviando fechas from_date : {now} , to_date : {now}")
    
    start_time = time.time()
    try:        
        results = {
            'kashio': get_data_kashio(now, now),
            'calimaco': get_data_calimaco(now, now)
        }
        
        if results['kashio'] and results['calimaco']:
            results['updated'] = updated_data_kashio()
        else:
            results['updated'] = False
        
        print(f"[debug] resultados: kashio={results['kashio']}, calimaco={results['calimaco']}, updated={results['updated']}")
    
        all_success = all(results.values())
        elapsed_time = time.time() - start_time
    
        if all_success:
            print(f"todas las operaciones completadas exitosamente en {elapsed_time:.2f} segundos")
            return {
                "success": True,
                "message": f"todas las operaciones completadas exitosamente en {elapsed_time:.2f} segundos",
                "failed_operations": []
            }
        else:
            
            failed_operations = []
            if not results['kashio']:
                failed_operations.append("kashio - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results.get('updated'):
                failed_operations.append("updated - no se pudo realizar la actualizacion")
            
            print(f"operaciones fallidas despues de {elapsed_time:.2f} segundos: {failed_operations}")
            return {
                "success": False,
                "message": "algunas operaciones fallaron",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }
        
    except Exception as e:
        error_message = f"error en get_main: {e}"
        print(error_message)
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }
        

if __name__ == "__main__":
    get_main_kashio('01032026', '04032026')