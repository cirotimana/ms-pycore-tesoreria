import asyncio
import time
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from app.digital.collectors.nuvei.analysis import get_data_nuvei, get_data_calimaco, conciliation_data, updated_data_nuvei
from app.common.utils import validate_date_range, format_duration


async def get_main_nuvei(from_date=None, to_date=None):
    # funcion principal que coordina la descarga y conciliacion de nuvei
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

    print(f"[debug] enviando fechas from_date: {from_date}, to_date: {to_date}")

    start_time = time.time()
    try:
        # ejecutar descarga de nuvei y calimaco en paralelo con asyncio.gather
        print("[info] iniciando descargas de nuvei y calimaco en paralelo...")
        
        # nuvei y calimaco son async, los ejecutamos concurrentemente
        results_list = await asyncio.gather(
            get_data_nuvei(from_date, to_date),
            get_data_calimaco(from_date, to_date),
            return_exceptions=True
        )

        results = {
            'nuvei': results_list[0] if not isinstance(results_list[0], Exception) else False,
            'calimaco': results_list[1] if not isinstance(results_list[1], Exception) else False
        }
        
        if isinstance(results_list[0], Exception): print(f"[error] excepcion en nuvei: {results_list[0]}")
        if isinstance(results_list[1], Exception): print(f"[error] excepcion en calimaco: {results_list[1]}")

        if results['nuvei'] and results['calimaco']:
            results['conciliation'] = conciliation_data(from_date, to_date)
        else:
            results['conciliation'] = False

        elapsed_time = time.time() - start_time
        print(f"[debug] resultados: nuvei={results['nuvei']}, calimaco={results['calimaco']}, conciliacion={results['conciliation']}")

        all_success = all(results.values())

        if all_success:
            print(f"[ok] Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos")
            return {
                "success": True,
                "message": f"Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos",
                "failed_operations": []
            }
        else:
            failed_operations = []
            if not results['nuvei']:
                failed_operations.append("nuvei - error en la descarga de datos")
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
        error_message = f"error en get_main_nuvei: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }


async def get_updated_nuvei():
    # funcion para actualizar datos del dia actual de nuvei y calimaco en paralelo
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)

    print(f"[debug] enviando fechas from_date: {now}, to_date: {now}")

    start_time = time.time()
    try:
        # ejecutar actualizacion de nuvei y calimaco en paralelo con asyncio.gather
        print("[info] iniciando actualizaciones de nuvei y calimaco en paralelo...")
        
        # nuvei y calimaco son async
        results_list = await asyncio.gather(
            get_data_nuvei(now, now),
            get_data_calimaco(now, now),
            return_exceptions=True
        )

        results = {
            'nuvei': results_list[0] if not isinstance(results_list[0], Exception) else False,
            'calimaco': results_list[1] if not isinstance(results_list[1], Exception) else False
        }
        
        if isinstance(results_list[0], Exception): print(f"[error] excepcion en nuvei: {results_list[0]}")
        if isinstance(results_list[1], Exception): print(f"[error] excepcion en calimaco: {results_list[1]}")

        if results['nuvei'] and results['calimaco']:
            results['updated'] = updated_data_nuvei()
        else:
            results['updated'] = False

        elapsed_time = time.time() - start_time
        print(f"[debug] resultados: nuvei={results['nuvei']}, calimaco={results['calimaco']}, updated={results['updated']}")

        all_success = all(results.values())

        if all_success:
            print(f"[ok] Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos")
            return {
                "success": True,
                "message": f"Todas las operaciones completadas exitosamente en {format_duration(elapsed_time)} minutos",
                "failed_operations": []
            }
        else:
            failed_operations = []
            if not results['nuvei']:
                failed_operations.append("nuvei - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results['updated']:
                failed_operations.append("updated - no se pudo realizar la actualizacion")

            print(f"[warn] operaciones fallidas despues de {elapsed_time:.2f} segundos: {failed_operations}")
            return {
                "success": False,
                "message": f"Algunas operaciones fallaron en {format_duration(elapsed_time)} minutos",
                "failed_operations": failed_operations,
                "successful_operations": [op for op, success in results.items() if success]
            }

    except Exception as e:
        elapsed_time = time.time() - start_time
        error_message = f"error en get_updated_nuvei: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }


if __name__ == "__main__":
    asyncio.run(get_main_nuvei('01032026', '10032026'))
