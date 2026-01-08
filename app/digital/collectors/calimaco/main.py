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
import time
from app.config import Config
import threading


# =============================
#   CACHE DE SESION CALIMACO
# =============================
class SessionCacheCalimaco:
    def __init__(self):
        self.session = None
        self.expires_at = None
        # self.lock = asyncio.Lock()
        self.lock = threading.Lock()

    async def get_session(self, force_refresh=False):
        # async with self.lock:
        with self.lock:
            now = datetime.now()

            if (not force_refresh and self.session and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando sesion Calimaco del cache")
                return self.session

            print("[INFO] Obteniendo nueva sesion Calimaco...")
            self.session = await get_cookies_calimaco()

            if self.session:
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Sesion cacheada hasta {self.expires_at}")

            return self.session

    def invalidate(self):
        self.session = None
        self.expires_at = None


session_cache_calimaco = SessionCacheCalimaco()


# =============================
#   LOGIN COMPATIBLE WINDOWS
# =============================
async def get_cookies_calimaco(max_attempts=10):
    print("[INFO] Iniciando navegador para obtener sesion Calimaco")

    browser = None
    context = None
    page = None
    session_value = None

    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Calimaco")
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
                nonlocal session_value
                if "isValidSession" in request.url and request.method == "POST":
                    post_data = request.post_data
                    if post_data:
                        params = urllib.parse.parse_qs(post_data)
                        session_value = params.get("session", [None])[0]
                        if session_value:
                            print(f"[DEBUG] Sesion capturada: {session_value[:15]}...")

            page.on("request", handle_request)

            for attempt in range(max_attempts):
                print(f"[INFO] Intento de login #{attempt + 1}")
                
                try:
                    await page.goto("https://bo.apuestatotal.com/login", 
                                  wait_until="networkidle", 
                                  timeout=60000)

                    await page.fill('input[name="alias"]', Config.USER_NAME_CALIMACO)
                    await page.fill('input[name="password"]', Config.PASSWORD_CALIMACO)
                    await page.click('button.btn.btn-primary')

                    for i in range(15):
                        if session_value:
                            print(f"[✔] Sesion capturada en intento {attempt + 1}")
                            return session_value
                        await asyncio.sleep(1)

                    print(f"[WARN] Sesion no capturada, reintentando...")
                    
                except Exception as e:
                    print(f"[ERROR] Error en intento {attempt + 1}: {e}")

            print("[✖] No se pudo capturar la sesion despues de todos los intentos.")
            return None

    except Exception as e:
        print(f"[ERROR] Error inesperado en get_cookies_calimaco: {e}")
        return None

    finally:
        print("[INFO] Cerrando recursos...")
        cleanup_tasks = []
        
        if page:
            cleanup_tasks.append(page.close())
        if context:
            cleanup_tasks.append(context.close())
        if browser:
            cleanup_tasks.append(browser.close())
            
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            print("[DEBUG] Recursos cerrados")


# =============================
#   DESCARGA DE REPORTES 
# =============================
async def get_wallet_report(session, from_date, to_date, method=None, collector = None):
    if not session or not method or not collector:
        print("[ERROR] Sesion, metodo o recaudador invalido.")
        return None


    url = "https://wallet.apuestatotal.com/api/admin_reports/getReport"
    headers = {
        "accept": "*/*",
        "content-type": "application/x-www-form-urlencoded",
    }

    start_date = from_date
    end_date = to_date + timedelta(days=1)
    current = start_date
    output_keys = []

    print(f"[INFO] Descargando transacciones de {start_date:%Y-%m-%d} a {to_date:%Y-%m-%d}")

    while current < end_date:
        from_dt = current.replace(hour=5, minute=0, second=0)
        to_dt = (current + timedelta(days=1)).replace(hour=5, minute=0, second=0)

        from_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"[INFO] Descargando transacciones dentro del bucle desde {from_str} a {to_str}")

        payload = {
            "session": session,
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
        
        
        # {"field": "t.status", "value": "in SUCCESS", "typeValue": "String", "type": "SelectMultiple", "useLikeFilter": True},

        success = False
        for retry in range(5):
            try:
                response = requests.post(url=url, headers=headers, data=payload, timeout=120)
                
                if response.status_code == 200:
                    content = response.content
                    
                    # Validar que el contenido no esté vacío
                    if not content or len(content) < 100:
                        print(f"[WARN] Contenido muy pequeño o vacío para {current.strftime('%Y-%m-%d')} (intento {retry + 1})")
                        if retry < 4:
                            await asyncio.sleep(10)
                            continue
                    
                    current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
                    output_key = f"digital/collectors/{collector}/calimaco/input/calimaco_{current_time}.csv"
                    
                    upload_file_to_s3(content, output_key)
                    output_keys.append(output_key)
                    print(f"[SUCCESS] Archivo guardado: {output_key}")
                    success = True
                    break
                    
                elif response.status_code == 401:
                    print(f"[ERROR] Sesion expirada (401)")
                    session_cache_calimaco.invalidate()
                    return None
                    
                else:
                    print(f"[ERROR] {response.status_code}: {response.text}")
                    if retry < 4:
                        await asyncio.sleep(10)
                    
            except Exception as e:
                print(f"[WARN] Error intento {retry + 1}: {e}")
                if retry < 4:
                    await asyncio.sleep(10)

        if not success:
            print(f"[ERROR] No se pudo descargar para {current.strftime('%Y-%m-%d')}")

        current += timedelta(days=1)
        await asyncio.sleep(1)

    return output_keys


# =============================
#   PROCESAMIENTO DE ARCHIVOS
# =============================
def process_calimaco_files(collector = None):
    if  not collector:
        print("[ERROR] No recaudador.")
        return None
    
    s3_client = get_s3_client_with_role()
    try:
        s3_prefix = f"digital/collectors/{collector}/calimaco/input/"
        s3_files = list_files_in_s3(s3_prefix)

        dataframes = []
        output_key = ""

        print(f"[INFO] Archivos encontrados: {len([f for f in s3_files if f.endswith('.csv') and '/input/processed/' not in f])}")
        
        for s3_key in s3_files:
            if s3_key.endswith('.csv') and '/input/processed/' not in s3_key:
                try:
                    print(f"[DEBUG] Procesando archivo: {s3_key}")
                    content = read_file_from_s3(s3_key)
                    
                    # Validar contenido
                    if not content or len(content) < 100:
                        print(f"[WARN] Archivo vacío o muy pequeño: {s3_key} (tamaño: {len(content) if content else 0} bytes)")
                        continue
                    
                    with BytesIO(content) as csv_data:
                        df = pd.read_csv(csv_data, 
                                        dtype={'Identifier': str}, 
                                        sep=',',
                                        encoding='utf-8',
                                        skiprows=1,
                                        low_memory=False)
                    
                    # Validar DataFrame
                    if df.empty:
                        print(f"[WARN] DataFrame vacío: {s3_key}")
                        continue
                    
                    print(f"[DEBUG] Columnas en {s3_key}: {list(df.columns)}")
                    
                    # Validar columnas esenciales
                    required_cols = ['Identifier', 'Status', 'Amount']
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    if missing_cols:
                        print(f"[WARN] Columnas faltantes en {s3_key}: {missing_cols}")
                        print(f"[DEBUG] Columnas disponibles: {list(df.columns)}")
                        continue
                    
                    dataframes.append(df)
                    print(f"[INFO] Procesado {s3_key}: {len(df)} registros")

                    # Mover a processed
                    if '/input/' in s3_key and '/input/processed/' not in s3_key:
                        new_key = s3_key.replace('/input/', '/input/processed/', 1)
                        s3_client.copy_object(
                            Bucket=Config.S3_BUCKET,
                            CopySource={'Bucket': Config.S3_BUCKET, 'Key': s3_key},
                            Key=new_key
                        )
                        delete_file_from_s3(s3_key)

                except Exception as e:
                    print(f"[ERROR] Error procesando {s3_key}: {e}")
                    continue

        if dataframes:
            print(f"[INFO] Consolidando {len(dataframes)} archivos de Calimaco...")
            consolidated_df = pd.concat(dataframes, ignore_index=True, copy=False)
            print(f"[INFO] Total registros consolidados: {len(consolidated_df)}")
            
            # Validar DataFrame consolidado
            if consolidated_df.empty:
                print("[ERROR] DataFrame consolidado está vacío")
                return None
            
            # Renombrar columnas
            consolidated_df = consolidated_df.rename(columns={
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
            })
            
            # Validar columnas críticas después del renombrado
            if 'ID' not in consolidated_df.columns or 'Estado' not in consolidated_df.columns:
                print("[ERROR] Columnas críticas faltantes después del renombrado")
                return None
            
            status_map = {
                'New': 'Nuevo',
                'Success': 'Válido',
                'LIMIT_EXCEEDED': 'Límites excedidos',
                'DENIED': 'Denegado'
            }
            
            consolidated_df["Estado"] = consolidated_df["Estado"].replace(status_map)
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/{collector}/calimaco/input/Calimaco_all_{collector}_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)

            print(f"[INFO] Archivo consolidado guardado: {output_key}")
        else:
            print("[✖] No se encontraron archivos CSV para consolidar.")
            
        return output_key

    except Exception as e:
        print(f"[✖] Error procesando datos de calimaco: {e}")
        return None


# =============================
#   FUNCION PRINCIPAL CORREGIDA
# =============================
async def get_main_data_async(from_date, to_date, method=None, collector= None):
    try:
        # await directo - sin asyncio.run() anidado
        session = await session_cache_calimaco.get_session(force_refresh=True)
        
        if not session:
            print("[ERROR] No se obtuvo sesion Calimaco valida.")
            return None

        # await directo  
        output_keys = await get_wallet_report(session, from_date, to_date, method, collector)
        
        if output_keys:
            print(f"[INFO] {len(output_keys)} archivos descargados, procesando...")
            # Llamada a funcion sync normal
            consolidated_file = process_calimaco_files(collector)
            return consolidated_file
        else:
            print("[WARN] No se descargaron archivos")
            return None

    except Exception as e:
        print(f"[ALERTA] Error en get_main_data_async: {e}")
        return None


def get_main_data(from_date, to_date, method=None, collector=None):
    print(f"[INICIO] Ejecutando Calimaco para {from_date} a {to_date}")
    result = asyncio.run(get_main_data_async(from_date, to_date, method, collector))
    print("[FIN] Proceso Calimaco completado")
    return result