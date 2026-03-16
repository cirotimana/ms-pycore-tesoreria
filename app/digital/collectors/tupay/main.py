from app.digital.collectors.tupay.analysis import *
from app.common.utils import validate_date_range, format_duration
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pytz
import time

def get_main_tupay(from_date = None, to_date = None):   
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    
    if not from_date or not to_date:
        from_date = now - timedelta(days=1)
        to_date = from_date
    else:
        fmt = '%d%m%Y' if len(from_date) == 8 else '%d%m%y'
        from_date = datetime.strptime(from_date, fmt).replace(tzinfo=lima_tz)
        to_date = datetime.strptime(to_date, fmt).replace(tzinfo=lima_tz)
        
    valid, from_date, to_date = validate_date_range(from_date, to_date)
    if not valid:
        return {"success": False, "message": "Rango o Formato invalido"}
    
    start_time = time.time()
    print(f"[debug] enviando fechas from_date : {from_date.date()} , to_date : {to_date.date()}")
    try:
        results = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_tupay = executor.submit(get_data_tupay, from_date, to_date)
            future_calimaco = executor.submit(get_data_calimaco, from_date, to_date)

            results = {
                'tupay': future_tupay.result(),
                'calimaco': future_calimaco.result()
            }
        
        if results['tupay'] and results['calimaco']:
            results['conciliation'] = conciliation_data(from_date, to_date)
        else:
            results['conciliation'] = False
        
        print(f"[debug] resultados: tupay={results['tupay']}, calimaco={results['calimaco']}, conciliacion={results['conciliation']}")
    
        all_success = all(results.values())
        elapsed_time = time.time() - start_time
    
        if all_success:
            print(f"[ok] Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos")
            return {
                "success": True,
                "message": f"Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos",
                "failed_operations": []
            }
        else:
            failed_operations = []
            if not results['tupay']:
                failed_operations.append("tupay - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results['conciliation']:
                failed_operations.append("conciliacion - no se pudo realizar la conciliacion")
            
            print(f"[warn] operaciones fallidas despues de {elapsed_time:.2f} segundos: {failed_operations}")
            return {
                "success": False,
                "message": f"Algunas operaciones fallaron en {format_duration(elapsed_time)} minutos",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        error_message = f"[error] error en get_main_tupay: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }
        

def get_updated_tupay():
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)

    start_time = time.time()
    print(f"[debug] enviando fechas from_date : {now} , to_date : {now}")
    try:
        results = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_tupay = executor.submit(get_data_tupay, now, now)
            future_calimaco = executor.submit(get_data_calimaco, now, now)

            results = {
                'tupay': future_tupay.result(),
                'calimaco': future_calimaco.result()
            }
        
        if results['tupay'] and results['calimaco']:
            results['updated'] = updated_data_tupay()
        else:
            results['updated'] = False
        
        print(f"[debug] resultados: tupay={results['tupay']}, calimaco={results['calimaco']}, updated={results['updated']}")
    
        all_success = all(results.values())
        elapsed_time = time.time() - start_time
    
        if all_success:
            print(f"[ok] Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos")
            return {
                "success": True,
                "message": f"Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos",
                "failed_operations": []
            }
        else:
            failed_operations = []
            if not results['tupay']:
                failed_operations.append("tupay - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results['updated']:
                failed_operations.append("updated - no se pudo realizar la actualizacion")
            
            print(f"[warn] operaciones fallidas despues de {elapsed_time:.2f} segundos: {failed_operations}")
            return  {
                "success": False,
                "message": f"Algunas operaciones fallaron en {format_duration(elapsed_time)} minutos",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        error_message = f"[error] error en get_updated_tupay: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }
        

if __name__ == "__main__":
    get_main_tupay('01032026', '10032026')

