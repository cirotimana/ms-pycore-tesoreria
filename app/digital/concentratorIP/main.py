from app.digital.concentratorIP.utils import (
    ensure_output_dir
)
from app.digital.concentratorIP.database import (
    get_total_ip_count,
    get_ip_data,
    save_to_database
)
from app.digital.concentratorIP.email_handler import (
    send_email_with_results,
    send_empty_results_email
)
from app.digital.concentratorIP.analysis import analyze_similar_emails
from app.digital.concentratorIP.concentratorIP_SQL import query
from app.common.database import engine_azure
import time

def get_main():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            print(f"ConcentratorIP - Intento {attempt + 1} de {max_retries}")
            
            # configuracion inicial
            engine = engine_azure
            output_dir = ensure_output_dir('concentratorip/attachments')
            
            # obtener datos
            df = get_ip_data(engine, query)
            total_ip = get_total_ip_count(engine)
            print(f"Total de registros: {total_ip}")
            
            if len(df) > 0:
                # analisis de datos
                df = analyze_similar_emails(df)
                
                # enviar resultados
                send_email_with_results(df, total_ip, output_dir)
                save_to_database(df, total_ip, len(df))
                print(f"ConcentratorIP - Ejecucion exitosa. {len(df)} registros procesados.")
                return {
                    "success": True,
                    "message": f"Ejecucion exitosa. {len(df)} registros procesados.",
                    "data_count": len(df)
                }
                
            else:
                # caso sin resultados
                send_empty_results_email(total_ip)
                save_to_database(None, total_ip, 0)
                print("ConcentratorIP - Ejecucion exitosa. Sin registros para procesar.")
                return {
                    "success": True,
                    "message": "Ejecucion exitosa. Sin registros para procesar.",
                    "data_count": 0
                }
                
            # Ejecucion exitosa - salir inmediatamente
            
        except Exception as e:
            print(f"ConcentratorIP - Error en intento {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"ConcentratorIP - Reintentando en 5 segundos... (intento {attempt + 2}/{max_retries})")
                time.sleep(5)
            else:
                print(f"ConcentratorIP - Fallo despues de {max_retries} intentos")
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

