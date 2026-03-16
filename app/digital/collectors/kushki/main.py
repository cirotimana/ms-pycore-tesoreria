from app.digital.collectors.kushki.analysis import *
from datetime import datetime, timedelta
import pytz
import time
from concurrent.futures import ThreadPoolExecutor
from app.common.utils import validate_date_range, format_duration


def get_main_kushki(from_date=None, to_date=None):
    # funcion principal que coordina la descarga y conciliacion de kushki
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
        # ejecutar descarga de kushki y calimaco en paralelo
        print("[info] iniciando descargas de kushki y calimaco en paralelo...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_kushki = executor.submit(get_data_kushki, from_date, to_date)
            future_calimaco = executor.submit(get_data_calimaco, from_date, to_date)

            # esperar a que ambos terminen antes de continuar
            results = {
                'kushki': future_kushki.result(),
                'calimaco': future_calimaco.result()
            }

        if results['kushki'] and results['calimaco']:
            results['conciliation'] = conciliation_data(from_date, to_date)
        else:
            results['conciliation'] = False

        elapsed_time = time.time() - start_time
        print(f"[debug] resultados: kushki={results['kushki']}, calimaco={results['calimaco']}, conciliacion={results['conciliation']}")

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
            if not results['kushki']:
                failed_operations.append("kushki - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results.get('conciliation'):
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
        error_message = f"error en get_main_kushki: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }


def get_updated_kushki():
    # funcion para actualizar datos del dia actual de kushki y calimaco en paralelo
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)

    print(f"[debug] enviando fechas from_date: {now}, to_date: {now}")

    start_time = time.time()
    try:
        # ejecutar actualizacion de kushki y calimaco en paralelo
        print("[info] iniciando actualizaciones de kushki y calimaco en paralelo...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_kushki = executor.submit(get_data_kushki, now, now)
            future_calimaco = executor.submit(get_data_calimaco, now, now)

            results = {
                'kushki': future_kushki.result(),
                'calimaco': future_calimaco.result()
            }

        if results['kushki'] and results['calimaco']:
            results['updated'] = updated_data_kushki()
        else:
            results['updated'] = False

        elapsed_time = time.time() - start_time
        print(f"[debug] resultados: kushki={results['kushki']}, calimaco={results['calimaco']}, updated={results['updated']}")

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
            if not results['kushki']:
                failed_operations.append("kushki - error en la descarga de datos")
            if not results['calimaco']:
                failed_operations.append("calimaco - error en la descarga de datos")
            if not results.get('updated'):
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
        error_message = f"error en get_updated_kushki: {e}"
        print(f"[error] {error_message} (despues de {elapsed_time:.2f} segundos)")
        return {
            "success": False,
            "message": error_message,
            "failed_operations": ["error general en el proceso"],
            "successful_operations": []
        }


if __name__ == "__main__":
    get_main_kushki('01032026', '05032026')
