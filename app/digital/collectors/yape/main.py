from app.digital.collectors.yape.analysis import *
from datetime import datetime, timedelta
import pytz

def get_main_yape(from_date = None, to_date = None):
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
    
    try:
        # Separar inserción de datos (rápido) de conciliación (lento)
        results = {
            'yape': get_data_yape(from_date, to_date),
            'calimaco': get_data_calimaco(from_date, to_date)
        }
        
        # Solo hacer conciliación si ambos datos están listos
        if results['yape'] and results['calimaco']:
            results['conciliation'] = conciliation_data(from_date, to_date)
        else:
            results['conciliation'] = False
        
        print(f"[DEBUG] Resultados: Yape={results['yape']}, Calimaco={results['calimaco']}, Conciliacion={results['conciliation']}")
    
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
            if not results['yape']:
                failed_operations.append("Yape - Error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("Calimaco - Error en la descarga de datos")
            if not results['conciliation']:
                failed_operations.append("Conciliacion - No se pudo realizar la conciliacion")
            
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
    


def get_updated_yape(from_date = None, to_date = None):
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    print(f"[DEBUG] Enviando fechas from_date : {now} , to_date : {now}")
    
    try:
        # Separar inserción de datos (rápido) de conciliación (lento)
        results = {
            'yape': get_data_yape_1(now, now),
            'calimaco': get_data_calimaco(now, now)
        }
        
        # Solo hacer conciliación si ambos datos están listos
        if results['yape'] and results['calimaco']:
            results['updated'] = updated_data_yape()
        else:
            results['updated'] = False
        
        print(f"[DEBUG] Resultados: Yape={results['yape']}, Calimaco={results['calimaco']}, Updated={results['updated']}")
    
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
            if not results['yape']:
                failed_operations.append("Yape - Error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("Calimaco - Error en la descarga de datos")
            if not results['updated']:
                failed_operations.append("Updated - No se pudo realizar la actulizacion")
            
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
    get_main_yape() 
