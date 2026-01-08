from app.digital.DNIcorrelatives.utils import (
    ensure_output_dir
)
from app.digital.DNIcorrelatives.database import (
    get_total_dni_count,
    get_dni_data,
    save_to_database
)
from app.digital.DNIcorrelatives.email_handler import (
    send_email_with_results,
    send_empty_results_email
)
from app.digital.DNIcorrelatives.analysis import analyze_correlative_dnis
from app.digital.DNIcorrelatives.DNIcorrelatives_SQL import query
from app.common.database import engine_azure
import time

def get_main():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            print(f"DNIcorrelatives - Intento {attempt + 1} de {max_retries}")
            
            # configuracion inicial
            engine = engine_azure
            output_dir = ensure_output_dir('correlativedni/attachments')
            
            # obtener datos
            df = get_dni_data(engine, query)
            total_dni = get_total_dni_count(engine)
            print(f"Total de registros: {total_dni}")
            
            if len(df) > 0:
                # analisis de datos
                df_final = analyze_correlative_dnis(df)
                
                # enviar resultados
                send_email_with_results(df_final, total_dni, output_dir)
                save_to_database(df_final, total_dni, len(df_final))
                print(f"DNIcorrelatives - Ejecucion exitosa. {len(df_final)} registros procesados.")
                return {
                    "success": True,
                    "message": f"Ejecucion exitosa. {len(df_final)} registros procesados.",
                    "data_count": len(df_final)
                }
            else:
                # caso sin resultados
                send_empty_results_email(total_dni)
                save_to_database(None, total_dni, 0)
                print("DNIcorrelatives - Ejecucion exitosa. Sin registros para procesar.")
                return {
                    "success": True,
                    "message": "Ejecucion exitosa. Sin registros para procesar.",
                    "data_count": 0
                }
                
            # Ejecucion exitosa - salir inmediatamente
            
        except Exception as e:
            print(f"DNIcorrelatives - Error en intento {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"DNIcorrelatives - Reintentando en 5 segundos... (intento {attempt + 2}/{max_retries})")
                time.sleep(5)
            else:
                print(f"DNIcorrelatives - Fallo despues de {max_retries} intentos")
                return {
                    "success": False,
                    "message": f"Fallo despues de {max_retries} intentos: {str(e)}"
                }
            
    return {
        "success": False,
        "message": "Se agotaron los reintentos sin exito."
    }


if __name__ =="__main__":
    get_main()
