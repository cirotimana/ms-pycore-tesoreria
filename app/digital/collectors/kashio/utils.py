import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import requests
from datetime import datetime, timedelta
import json
import pytz
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *
import time
from contextlib import contextmanager


# =============================
#   CACHE DE TOKEN KASHIO
# =============================
class TokenCacheKashio:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.token and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token Kashio del cache")
                return self.token

            print("[INFO] Obteniendo nuevo token Kashio...")
            self.token = await get_token_kashio()
            
            if self.token:
                # Token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token Kashio cacheado hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nuevo token Kashio")
            
            return self.token

    def invalidate(self):
        print("[INFO] Invalidando token Kashio cacheado")
        self.token = None
        self.expires_at = None


token_cache_kashio = TokenCacheKashio()


# =============================
#   OBTENER TOKEN 
# =============================

async def get_token_kashio():
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Kashio")
            browser = await p.chromium.launch(
                headless=True,  
                # args=[
                #     "--no-sandbox",
                #     "--disable-dev-shm-usage", 
                #     "--disable-gpu",
                #     "--no-zygote",
                #     "--single-process",
                # ]
            )
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
            )
            
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()
            token_found = None

            async def handle_request(route):
                nonlocal token_found
                headers = route.request.headers
                if 'authorization' in headers and not token_found:
                    token_found = headers['authorization']
                    print(f"[DEBUG] Token capturado: {token_found[:20]}...")
                await route.continue_()

            await context.route("**/design.configuration*", handle_request)
            
            print("[INFO] Navegando a Kashio")
            await page.goto("https://kpms.kashio.net/", wait_until='networkidle', timeout=60000)

            # Verificar errores de carga
            page_content = await page.content()
            if "Oops" in page_content:
                print("[ERROR] Error detectado al cargar la pagina KashIO")
                return None

            # NUEVA FUNCION PARA HACER LOGIN DE FORMA ROBUSTA
            async def intentar_login():
                try:
                    # Esperar un poco para que la pagina cargue completamente
                    await asyncio.sleep(3)
                    
                    # ESTRATEGIA 1: Esperar explicitamente por los campos
                    print("[DEBUG] Esperando campos de login...")
                    try:
                        await page.wait_for_selector('input[name="email"]', state='visible', timeout=10000)
                        await page.wait_for_selector('input[name="password"]', state='visible', timeout=10000)
                        await page.wait_for_selector('#login-submit-button', state='visible', timeout=10000)
                        print("[DEBUG] Campos de login encontrados (selectores estandar)")
                    except:
                        print("[WARN] No se encontraron con selectores estandar, intentando alternativas...")
                        
                        # ESTRATEGIA 2: Buscar por tipo de input
                        email_input = await page.query_selector('input[type="email"]')
                        password_input = await page.query_selector('input[type="password"]')
                        submit_button = await page.query_selector('button[type="submit"]')
                        
                        if not (email_input and password_input and submit_button):
                            print("[ERROR] No se pudieron encontrar los campos de login")
                            return False
                    
                    # Verificar visibilidad
                    email_visible = await page.is_visible('input[name="email"]') or await page.is_visible('input[type="email"]')
                    password_visible = await page.is_visible('input[name="password"]') or await page.is_visible('input[type="password"]')
                    button_visible = await page.is_visible('#login-submit-button') or await page.is_visible('button[type="submit"]')
                    
                    print(f"[DEBUG] Email visible: {email_visible}")
                    print(f"[DEBUG] Password visible: {password_visible}")
                    print(f"[DEBUG] Boton visible: {button_visible}")
                    
                    if not (email_visible and password_visible and button_visible):
                        print("[WARN] Algunos campos no visibles, esperando mas tiempo...")
                        await asyncio.sleep(3)
                    
                    # ESTRATEGIA 3: Llenar con multiples intentos
                    print("[INFO] Llenando campo email...")
                    
                    # Intentar por name primero
                    try:
                        await page.fill('input[name="email"]', Config.USER_NAME_KASHIO, timeout=5000)
                    except:
                        # Fallback a tipo
                        await page.fill('input[type="email"]', Config.USER_NAME_KASHIO, timeout=5000)
                    
                    # Pequeña pausa
                    await asyncio.sleep(0.5)
                    
                    print("[INFO] Llenando campo password...")
                    try:
                        await page.fill('input[name="password"]', Config.PASSWORD_KASHIO, timeout=5000)
                    except:
                        await page.fill('input[type="password"]', Config.PASSWORD_KASHIO, timeout=5000)
                    
                    # Pequeña pausa antes de hacer click
                    await asyncio.sleep(0.5)
                    
                    print("[INFO] Haciendo click en login...")
                    try:
                        await page.click('#login-submit-button', timeout=5000)
                    except:
                        await page.click('button[type="submit"]', timeout=5000)
                    
                    return True
                    
                except Exception as e:
                    print(f"[ERROR] Error en intentar_login: {e}")
                    return False
            
            # Hacer el primer intento de login
            login_success = await intentar_login()
            if not login_success:
                print("[ERROR] Fallo el primer intento de login")
                return None
            
            # Esperar redireccion
            try:
                await page.wait_for_url("**/home**", timeout=15000)
                print("[INFO] Login exitoso - Redirigido a home")
            except:
                print("[INFO] No hubo redireccion inmediata despues del login")

            # Esperar token
            max_wait_cycles = 10       
            wait_per_cycle = 60

            print(f"[INFO] Esperando captura del token (hasta {max_wait_cycles * wait_per_cycle}s totales)")
            for attempt in range(max_wait_cycles):
                for i in range(wait_per_cycle):
                    if token_found:
                        print(f"[SUCCESS] Token capturado en segundo {attempt * wait_per_cycle + i + 1}")
                        break
                    await asyncio.sleep(1)

                if token_found:
                    break
                else:
                    print(f"[WARN] Intento {attempt + 1}/{max_wait_cycles} sin token. Refrescando pagina y reintentando login...")
                    try:
                        await page.reload(wait_until='networkidle', timeout=30000)
                        
                        # Reintentar login despues del reload
                        await intentar_login()
                        
                    except Exception as e:
                        print(f"[ERROR] Fallo al refrescar intento {attempt + 1}: {e}")
                    await asyncio.sleep(5)

            if token_found:
                print("[INFO] Token capturado exitosamente")
                return token_found
            else:
                print("[ERROR] No se encontro token despues de reintentos")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_kashio: {e}")
        return None
        
    finally:
        await close_playwright_resources_kashio(browser, context, page)

async def close_playwright_resources_kashio(browser, context, page):
    print("[INFO] Cerrando recursos de Kashio...")

    cleanup_tasks = [] 
    if page:
        cleanup_tasks.append(page.close()) 
        
    if context: 
        cleanup_tasks.append(context.close()) 
        
    if browser:
        cleanup_tasks.append(browser.close()) 

    if cleanup_tasks: 
        try: 
            await asyncio.gather(*cleanup_tasks, return_exceptions=True) 
            print("[DEBUG] Recursos de Kashio cerrados correctamente")
        except Exception as e: 
            print(f"[WARN] Error cerrando recursos de Kashio: {e}")

# =============================
#   FUNCIONES DE DATOS 
# =============================
async def download_day_data(current_day, headers, url, limit, semaphore):
    # funcion auxiliar para descargar la data de un solo dia con paginacion y limite de concurrencia
    async with semaphore:
        print(f"[info] iniciando descarga para {current_day.date()} (concurrencia controlada)")
        
        from_dt = current_day.replace(hour=5, minute=0, second=0)
        to_dt = (current_day + timedelta(days=1)).replace(hour=5, minute=0, second=0)
        from_date_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        all_day_data = []
        offset = 0
        has_more_data = True
        
        while has_more_data:
            params = {
                "from_date": from_date_str,
                "to_date": to_date_str,
                "limit": limit,
                "start": offset
            }
            
            max_retries = 30
            success = False
            registros = []
            
            for retry in range(max_retries):
                try:
                    # usamos to_thread para evitar bloquear el event loop con requests (que es sincrono)
                    response = await asyncio.to_thread(requests.get, url, headers=headers, params=params, timeout=500)
                    
                    if response.status_code == 200:
                        data = response.json()
                        registros = data.get('data') if isinstance(data, dict) and 'data' in data else data
                            
                        if registros:
                            all_day_data.extend(registros)
                            print(f"[info] {current_day.date()} | descargados {len(registros)} registros (offset {offset})")
                            
                            if len(registros) < limit:
                                has_more_data = False
                            else:
                                offset += limit
                            
                            success = True
                            break
                        else:
                            print(f"[info] {current_day.date()} | no hay más registros")
                            has_more_data = False
                            success = True
                            break
                            
                    elif response.status_code in [401, 403]:
                        print(f"[error] {current_day.date()} | error de autorizacion {response.status_code}")
                        return None
                        
                    elif response.status_code in [502, 504]:
                        wait_time = (retry + 1) * 10 # exponencial basico
                        print(f"[warn] {current_day.date()} | error {response.status_code}, reintento {retry + 1}, esperando {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        print(f"[error] {current_day.date()} | status {response.status_code}: {response.text[:100]}")
                        has_more_data = False
                        break
                        
                except Exception as e:
                    print(f"[error] {current_day.date()} | excepcion: {e}")
                    if retry < max_retries - 1:
                        await asyncio.sleep(10)
                    else:
                        has_more_data = False
                        break
            
            if not success:
                break
                
            if has_more_data:
                await asyncio.sleep(1)
        
        if all_day_data:
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            ms = datetime.now().strftime('%f')
            file_key = f"digital/collectors/kashio/input/response_{current_time}_{ms}.json"
            upload_file_to_s3(json.dumps(all_day_data, ensure_ascii=False).encode("utf-8"), file_key)
            print(f"[success] {current_day.date()} | guardado en s3: {file_key}")
            return len(all_day_data)
        
        return 0


async def get_data_json_kashio_async(token, from_date, to_date):
    start_date = from_date
    end_date = to_date + timedelta(days=1)
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }
    
    url = "https://ns10-api-web-extranet.kashio.com.pe/kcms/v1/customers/cus_TZgE7VA6xSxTjmN8eutJcm/payments"
    limit = 500
    
    # limitamos la concurrencia a 2 solicitudes simultaneas para evitar saturar el servidor (error 504)
    semaphore = asyncio.Semaphore(2)

    days_to_process = []
    current_day = start_date
    while current_day < end_date:
        days_to_process.append(current_day)
        current_day += timedelta(days=1)

    print(f"[info] iniciando descarga controlada para {len(days_to_process)} dias (max 2 hilos)")
    
    tasks = [download_day_data(day, headers, url, limit, semaphore) for day in days_to_process]
    results = await asyncio.gather(*tasks)
    
    total_records = sum(r for r in results if r is not None)
    
    print(f"[info] total general de registros descargados: {total_records}")
    return total_records, token


async def process_single_json_file(s3_key, s3_client):
    # funcion auxiliar para procesar un solo json a excel en paralelo
    try:
        print(f"[info] procesando {s3_key}...")
        content = read_file_from_s3(s3_key)
        data = json.loads(content.decode("utf-8"))

        if not data:
            print(f"[warn] archivo vacio: {s3_key}")
            return False

        # Transformar los datos para el DataFrame
        rows = []
        for item in data:
            invoice = item.get("invoice_list", [{}])[0] if item.get("invoice_list") else {}
            row = {
                "FECHA DE REGISTRO": (
                    pd.to_datetime(item.get("created"), errors="coerce") - timedelta(hours=5)
                ).strftime("%d/%m/%Y %H:%M:%S") if item.get("created") else "",
                "REFERENCIA DE PAGO": item.get("reference", ""),
                "CLIENTE": item.get("customer", {}).get("name", ""),
                "REFERENCIA DE ORDEN": invoice.get("external_id", ""),
                "DESCRIPCION": invoice.get("name", ""),
                "SUBTOTAL": invoice.get("sub_total", {}).get("value", ""),
                "MORA": invoice.get("late_fee", {}).get("value", ""),
                "TOTAL": invoice.get("total", {}).get("value", ""),
                "METODO DE PAGO": item.get("metadata", {}).get("psp_account", {}).get("name", ""),
                "OPERACION": "Pago",
                "ESTADO": item.get("status", "")
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)

        # Limpiar referencia de orden
        df['REFERENCIA DE ORDEN'] = (
            df['REFERENCIA DE ORDEN']
            .str.replace('-ATP', '', regex=False)
            .str.replace('-', '.', regex=False)
        )

        column_order = [
            "FECHA DE REGISTRO", "REFERENCIA DE PAGO", "CLIENTE", "REFERENCIA DE ORDEN",
            "DESCRIPCION", "SUBTOTAL", "MORA", "TOTAL", "METODO DE PAGO", "OPERACION", "ESTADO"
        ]
        df = df[column_order]

        excel_key = s3_key.replace(".json", ".xlsx")

        with bytes_io_context() as buffer:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), excel_key)

        # mover a processed
        processed_key = s3_key.replace("/input/", "/input/processed/", 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={"Bucket": Config.S3_BUCKET, "Key": s3_key},
            Key=processed_key,
        )
        delete_file_from_s3(s3_key)
        print(f"[success] procesado: {s3_key} -> {excel_key}")
        return True
    except Exception as e:
        print(f"[error] error procesando {s3_key}: {e}")
        return False


async def json_excel_kashio_async():
    # version asincrona para procesar archivos en paralelo
    try:
        s3_prefix = "digital/collectors/kashio/input/"
        s3_files = list_files_in_s3(s3_prefix)
        s3_client = get_s3_client_with_role()

        files_to_process = [
            f for f in s3_files 
            if f.endswith(".json") and "/input/processed/" not in f
        ]

        if not files_to_process:
            print("[info] no hay archivos json para procesar")
            return

        print(f"[info] procesando {len(files_to_process)} archivos en paralelo...")
        tasks = [process_single_json_file(f, s3_client) for f in files_to_process]
        await asyncio.gather(*tasks)
        
        print("[success] proceso json -> excel completado")
    except Exception as e:
        print(f"[error] error en json_excel_kashio: {e}")


@contextmanager
def bytes_io_context():
    buffer = BytesIO()
    try:
        yield buffer
    finally:
        buffer.close()


def json_excel_kashio():
    # wrapper sincrono para mantener compatibilidad
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # si ya estamos en un loop (como en get_data_main_async), usamos un task
            asyncio.create_task(json_excel_kashio_async())
        else:
            asyncio.run(json_excel_kashio_async())
    except Exception as e:
        print(f"[error] error al lanzar json_excel_kashio: {e}")


# =============================
#   FUNCION PRINCIPAL 
# =============================
async def get_data_main_async(from_date, to_date):
    try:
        print(f"[INICIO] Procesando Kashio desde {from_date} hasta {to_date}")
        
        # obtener token del cache
        token = await token_cache_kashio.get_token()
        if not token:
            print("[error] no se pudo obtener token de kashio")
            return False

        # descargar datos en paralelo
        data_count, final_token = await get_data_json_kashio_async(token, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[info] {data_count} registros descargados, generando archivo excel")
            await json_excel_kashio_async()
            print("[success] proceso kashio completado exitosamente")
            return True
        else:
            print("[info] no hay datos para procesar")
            return False
        
    except Exception as e:
        print(f"[error] error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    # wrapper que asegura el formato de fechas
    start_time = time.time()

    print(f"\n{'='*50}")
    print(f"[inicio] proceso kashio | rango: {from_date.date()} a {to_date.date()}")
    print(f"{'='*50}\n")
    
    try:
        result = asyncio.run(get_data_main_async(from_date, to_date))
    except Exception as e:
        print(f"[error] fallo ejecucion principal kashio: {e}")
        result = False
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        print(f"\n{'='*50}")
        print(f"[fin] proceso kashio completado")
        print(f"[tiempo] duracion total: {elapsed_time:.2f} segundos")
        print(f"{'='*50}\n")
    
    return result

def validate_date_range(from_date, to_date):
    try:
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%d%m%y")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%d%m%y")
    except Exception as e:
        print(f"[error] formato de fecha invalido en kashio: {e}")
        return False, None, None

    # validar rango maximo de 10 dias (conteo inclusivo)
    try:
        days_diff = (to_date.replace(tzinfo=None) - from_date.replace(tzinfo=None)).days + 1
        if days_diff > 10:
            print(f"[error] kashio: el rango solicitado ({days_diff} dias) excede el maximo de 10 dias")
            return False, None, None
    except Exception as e:
        print(f"[error] calculando el rango en kashio: {e}")
        return False, None, None
    
    return True, from_date, to_date
