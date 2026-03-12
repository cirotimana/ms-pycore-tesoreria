import requests
import json
from datetime import datetime, timedelta
from app.common.s3_utils import *
import pandas as pd
from io import BytesIO
import pytz
import asyncio
from playwright.async_api import async_playwright
import urllib.parse
import time as _time
from app.config import Config
import threading


# =============================
#   cache de sesion calimaco
# =============================
class SessionCacheCalimaco:
    def __init__(self):
        self.session = None
        self.expires_at = None
        self.lock = threading.Lock()

    async def get_session(self, force_refresh=False):
        # obtiene la sesion del cache o inicia una nueva
        with self.lock:
            now = datetime.now()

            if (not force_refresh and self.session and 
                self.expires_at and now < self.expires_at):
                print("[info calimaco] usando sesion calimaco del cache")
                return self.session

            print("[info calimaco] obteniendo nueva sesion calimaco...")
            self.session = await get_session_cookies()

            if self.session:
                self.expires_at = now + timedelta(minutes=30)
                print(f"[info calimaco] sesion cacheada hasta {self.expires_at}")

            return self.session

    def invalidate(self):
        # invalida la sesion actual
        self.session = None
        self.expires_at = None


session_cache = SessionCacheCalimaco()


# =============================
#   obtencion de cookies
# =============================
async def get_session_cookies(max_attempts=10):
    # inicia navegador para capturar la sesion
    print("[info calimaco] iniciando navegador para obtener sesion calimaco")

    browser = None
    context = None
    page = None
    captured_session = None

    try:
        async with async_playwright() as p:
            print("[info calimaco] lanzando navegador")
            browser = await p.chromium.launch(
                headless=True,
                args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--no-zygote",
                        "--single-process",
                    ]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { 
                    get: () => undefined 
                });
            """)

            page = await context.new_page()

            async def handle_request(request):
                # captura el token de sesion desde las peticiones de red
                nonlocal captured_session
                if "isValidSession" in request.url and request.method == "POST":
                    post_data = request.post_data
                    if post_data:
                        params = urllib.parse.parse_qs(post_data)
                        captured_session = params.get("session", [None])[0]
                        if captured_session:
                            print(f"[debug calimaco] sesion capturada: {captured_session[:15]}...")

            page.on("request", handle_request)

            for attempt in range(max_attempts):
                print(f"[info calimaco] intento de login #{attempt + 1}")
                
                try:
                    await page.goto("https://bo.apuestatotal.com/login", 
                                  wait_until="networkidle", 
                                  timeout=60000)

                    await page.fill('input[name="alias"]', Config.USER_NAME_CALIMACO)
                    await page.fill('input[name="password"]', Config.PASSWORD_CALIMACO)
                    await page.click('button.btn.btn-primary')

                    for _ in range(15):
                        if captured_session:
                            print(f"[ok calimaco] sesion capturada en intento {attempt + 1}")
                            return captured_session
                        await asyncio.sleep(1)

                    print(f"[warn calimaco] sesion no capturada, reintentando...")
                    
                except Exception as e:
                    print(f"[error calimaco] error en intento {attempt + 1}: {e}")

            print("[error calimaco] no se pudo capturar la sesion")
            return None

    except Exception as e:
        print(f"[error calimaco] error inesperado: {e}")
        return None

    finally:
        print("[info calimaco] cerrando recursos de navegador")
        cleanup_tasks = []
        
        if page:
            cleanup_tasks.append(page.close())
        if context:
            cleanup_tasks.append(context.close())
        if browser:
            cleanup_tasks.append(browser.close())
            
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            print("[debug calimaco] recursos cerrados")


# =============================
#   descarga de reportes
# =============================
async def download_wallet_report(session_token, from_date, to_date, method=None, collector_name=None):
    # descarga reporte de transacciones en un solo rango
    if not session_token or not method or not collector_name:
        print("[error calimaco] parametros invalidos para descarga")
        return False

    api_url = "https://wallet.apuestatotal.com/api/admin_reports/getReport"
    request_headers = {
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded",
    }

    # configurar rango: desde las 05:00 del primer dia hasta las 05:00 del dia siguiente al ultimo
    from_str = from_date.replace(hour=5, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
    to_str = (to_date + timedelta(days=1)).replace(hour=5, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[info calimaco] descargando rango completo desde {from_str} hasta {to_str}")

    payload = {
        "session": session_token,
        "company": "ATP",
        "report": "deposits",
        "filter": json.dumps([
            {"field": "updated_date", "value": [from_str, to_str], "type": "time_range"},
            {"field": "t.method", "value": f"in {method}", "typeValue": "String", "type": "SelectMultiple", "useLikeFilter": True},
        ]),
        "limit": "",
        "lang": "en",
        "csv": "true",
        "callFilter": "",
    }
    
    for retry in range(5):
        try:
            print(f"[info calimaco] enviando solicitud a calimaco (intento {retry + 1}/5), esperando respuesta (timeout 300s)...")
            response = requests.post(url=api_url, headers=request_headers, data=payload, timeout=300)
            
            if response.status_code == 200:
                file_content = response.content
                
                if not file_content or len(file_content) < 100:
                    print(f"[warn calimaco] contenido insuficiente del reporte (intento {retry + 1})")
                    if retry < 4:
                        print("[info calimaco] reintentando en 10 segundos...")
                        await asyncio.sleep(10)
                        continue
                
                timestamp_str = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
                s3_output_key = f"digital/collectors/{collector_name}/calimaco/input/calimaco_{timestamp_str}.csv"
                
                upload_file_to_s3(file_content, s3_output_key)
                print(f"[ok calimaco] reporte consolidado guardado en s3: {s3_output_key}")
                return s3_output_key
                
            elif response.status_code == 401:
                print(f"[error calimaco] sesion expirada (401)")
                session_cache.invalidate()
                return False
                
            else:
                print(f"[error calimaco] error {response.status_code}: {response.text}")
                if retry < 4:
                    await asyncio.sleep(10)
                
        except Exception as e:
            print(f"[warn calimaco] error en intento {retry + 1}: {e}")
            if retry < 4:
                await asyncio.sleep(10)

    print(f"[error calimaco] no se pudo descargar el reporte de rango completo")
    return False


# =============================
#   procesamiento de datos
# =============================
def process_extracted_files(collector_name=None, specific_file_key=None):
    # procesa y consolida archivos csv. si se provee una key especifica, se procesa solo esa.
    if not collector_name:
        print("[error calimaco] nombre de recaudador no proporcionado")
        return False
    
    s3_client = get_s3_client_with_role()
    try:
        extracted_dfs = []
        final_csv_key = ""

        # si tenemos una key especifica, no listamos todo el s3
        if specific_file_key:
            files_to_process = [specific_file_key]
            print(f"[info calimaco] procesando directamente el archivo: {specific_file_key}")
        else:
            input_prefix = f"digital/collectors/{collector_name}/calimaco/input/"
            files_to_process = [f for f in list_files_in_s3(input_prefix) if f.endswith('.csv') and '/input/processed/' not in f]
            print(f"[info calimaco] buscando archivos para procesar en {input_prefix}")
        
        for file_key in files_to_process:
            try:
                print(f"[debug calimaco] leyendo archivo: {file_key}")
                raw_content = read_file_from_s3(file_key)
                
                if not raw_content or len(raw_content) < 100:
                    print(f"[warn calimaco] archivo vacio o corrupto: {file_key}")
                    continue
                
                with BytesIO(raw_content) as csv_buffer:
                    temp_df = pd.read_csv(csv_buffer, 
                                        dtype={'Identifier': str}, 
                                        sep=',',
                                        encoding='utf-8',
                                        skiprows=1,
                                        low_memory=False)
                
                if temp_df.empty:
                    print(f"[warn calimaco] dataframe vacio para {file_key}")
                    continue
                
                required_columns = ['Identifier', 'Status', 'Amount']
                missing_columns = [col for col in required_columns if col not in temp_df.columns]
                if missing_columns:
                    print(f"[warn calimaco] faltan columnas en {file_key}: {missing_columns}")
                    continue
                
                extracted_dfs.append(temp_df)
                print(f"[info calimaco] procesado correctamente {file_key}")

                # mover archivo a carpeta de procesados
                processed_key = file_key.replace('/input/', '/input/processed/', 1)
                s3_client.copy_object(
                    Bucket=Config.S3_BUCKET,
                    CopySource={'Bucket': Config.S3_BUCKET, 'Key': file_key},
                    Key=processed_key
                )
                delete_file_from_s3(file_key)

            except Exception as e:
                print(f"[error calimaco] error procesando {file_key}: {e}")
                continue

        if extracted_dfs:
            print(f"[info calimaco] consolidando {len(extracted_dfs)} archivos...")
            consolidated_df = pd.concat(extracted_dfs, ignore_index=True, copy=False)
            
            if consolidated_df.empty:
                print("[error calimaco] consolidacion resulto en dataframe vacio")
                return False
            
            # mapeo de columnas a nombres legibles
            column_mapping = {
                'Identifier': 'ID',
                'Date': 'Fecha', 
                'Status': 'Estado',
                'Updated date': 'Fecha de modificación',
                'User': 'Usuario',
                'email': 'email', 
                'Amount': 'Cantidad',
                'External ID': 'ID externo',
                'Method': 'Método',
                'Response': 'Respuesta',
                'Agent': 'Agente',
                'User register date': 'Fecha de registro del jugador',  
                'Comments': 'Comentarios'
            }
            consolidated_df = consolidated_df.rename(columns=column_mapping)
            
            status_translations = {
                'New': 'Nuevo',
                'Success': 'Válido',
                'LIMIT_EXCEEDED': 'Límites excedidos',
                'DENIED': 'Denegado'
            }
            if 'Estado' in consolidated_df.columns:
                consolidated_df["Estado"] = consolidated_df["Estado"].replace(status_translations)
            
            # guardar resultado consolidado en s3
            current_timestamp = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            final_csv_key = f"digital/collectors/{collector_name}/calimaco/input/Calimaco_all_{collector_name}_{current_timestamp}.csv"

            with BytesIO() as output_buffer:
                consolidated_df.to_csv(output_buffer, index=False)
                output_buffer.seek(0)
                upload_file_to_s3(output_buffer.getvalue(), final_csv_key)

            print(f"[info calimaco] archivo final guardado: {final_csv_key}")
        else:
            print("[warn calimaco] no se encontraron datos para procesar")
            
        return final_csv_key

    except Exception as e:
        print(f"[error calimaco] falla en procesamiento de archivos: {e}")
        return False


# =============================
#   flujo principal
# =============================
async def run_calimaco_collector_async(from_date, to_date, method=None, collector_name=None):
    # orquestador asincrono del proceso de extraccion con control de tiempo
    start_time = _time.time()

    print(f"\n{'='*50}")
    print(f"[inicio calimaco] proceso calimaco | rango: {from_date.date()} a {to_date.date()}")
    print(f"{'='*50}\n")

    try:
        session_token = await session_cache.get_session(force_refresh=True)

        if not session_token:
            print("[error calimaco] no se pudo establecer sesion con calimaco")
            return False

        file_key = await download_wallet_report(session_token, from_date, to_date, method, collector_name)

        if file_key:
            print(f"[info calimaco] archivo descargado, iniciando procesamiento directo")
            result = process_extracted_files(collector_name, specific_file_key=file_key)
        else:
            print("[warn calimaco] no se pudo descargar el archivo del rango solicitado")
            result = False

    except Exception as e:
        print(f"[error calimaco] error general en flujo asincrono: {e}")
        result = False

    finally:
        elapsed_time = _time.time() - start_time
        print(f"\n{'='*50}")
        print(f"[fin calimaco] proceso calimaco completado")
        print(f"[tiempo calimaco] duracion total: {elapsed_time / 60:.2f} minutos")
        print(f"{'='*50}\n")

    return result


def get_main_data(from_date, to_date, method=None, collector=None):
    # wrapper sincrono que ejecuta el proceso principal de calimaco
    try:
        result = asyncio.run(run_calimaco_collector_async(from_date, to_date, method, collector))
    except Exception as e:
        print(f"[error calimaco] fallo ejecucion principal: {e}")
        result = False
    return result

