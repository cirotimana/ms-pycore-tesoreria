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
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage", 
                    "--disable-gpu",
                    "--no-zygote",
                    "--single-process",
                ]
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

    total_data = 0
    current_token = token

    current_day = start_date
    while current_day < end_date:
        print(f"[INFO] Descargando transacciones para {current_day.date()}")
        
        from_dt = current_day.replace(hour=5, minute=0, second=0)
        to_dt = (current_day + timedelta(days=1)).replace(hour=5, minute=0, second=0)
        from_date_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        all_data = []
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
                    response = requests.get(url, headers=headers, params=params, timeout=500)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, dict) and 'data' in data:
                            registros = data['data']
                        else:
                            registros = data
                            
                        if registros:
                            all_data.extend(registros)
                            print(f"[INFO] Descargados {len(registros)} registros (offset {offset})")
                            
                            # Verificar si hay mas datos
                            if len(registros) < limit:
                                has_more_data = False
                            else:
                                offset += limit
                            
                            success = True
                            break
                        else:
                            print(f"[INFO] No hay mas registros para {current_day.date()}")
                            has_more_data = False
                            success = True
                            break
                            
                    elif response.status_code in [401, 403]:
                        print(f"[ERROR] Error de autorizacion {response.status_code}")
                        if retry < max_retries - 1:
                            new_token = await token_cache_kashio.get_token(force_refresh=True)
                            if new_token:
                                current_token = new_token
                                headers["Authorization"] = current_token
                                print("[INFO] Token renovado para descarga")
                            else:
                                print("[ERROR] No se pudo renovar token")
                        break
                        
                    elif response.status_code in [502, 504]:
                        print(f"[WARN] Error {response.status_code} en offset {offset}, reintento {retry + 1}")
                        if retry < max_retries - 1:
                            await asyncio.sleep(5)
                        else:
                            has_more_data = False
                            break
                            
                    else:
                        print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
                        has_more_data = False
                        break
                        
                except Exception as e:
                    print(f"[ERROR] Excepcion durante descarga: {e}")
                    if retry < max_retries - 1:
                        await asyncio.sleep(5)
                    else:
                        has_more_data = False
                        break
            
            if not success:
                print(f"[ERROR] No se pudo descargar datos para offset {offset}")
                break
            
            # Pequena pausa entre requests
            if has_more_data:
                await asyncio.sleep(1)
        
        print(f"[INFO] Descarga completa para {current_day.date()}, total registros: {len(all_data)}")
        total_data += len(all_data)
        
        # Guardar datos del dia
        if all_data:
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            file_key = f"digital/collectors/kashio/input/response_{current_time}.json"
            upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
            print(f"[SUCCESS] Archivo guardado en S3: {file_key}")
            
        current_day += timedelta(days=1)
        
        # Pausa entre dias
        if current_day < end_date:
            await asyncio.sleep(2)
            
    print(f"[INFO] Total general de registros descargados: {total_data}")
    return total_data, current_token


def json_excel_kashio():
    
    prefix = "digital/collectors/kashio/input/"
    files = list_files_in_s3(prefix)
    
    processed_count = 0

    for file_key in files:
        if not file_key.endswith(".json"):
            continue
        
        if "/input/processed/" in file_key:
            continue  

        print(f"[INFO] Procesando {file_key}...")
        try:
            content = read_file_from_s3(file_key)
            data = json.loads(content.decode("utf-8"))

            if isinstance(data, dict):
                data = data.get("data") or data.get("results") or data.get("invoices") or []
            
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

            # Guardar Excel
            with BytesIO() as excel_buffer:
                df.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                output_key = file_key.replace(".json", ".xlsx")
                upload_file_to_s3(excel_buffer.getvalue(), output_key)
                
            # Mover JSON a processed
            processed_key = file_key.replace("input/", "input/processed/", 1)
            upload_file_to_s3(content, processed_key)
            delete_file_from_s3(file_key)

            processed_count += 1
            print(f"[SUCCESS] Procesado: {file_key} -> {output_key}")

        except Exception as e:
            print(f"[ERROR] Error procesando {file_key}: {e}")
            continue

    print(f"[INFO] Proceso JSON -> Excel completado. Archivos procesados: {processed_count}")


# =============================
#   FUNCION PRINCIPAL 
# =============================
async def get_data_main_async(from_date, to_date):
    try:
        print(f"[INICIO] Procesando Kashio desde {from_date} hasta {to_date}")
        
        # Obtener token del cache
        token = await token_cache_kashio.get_token()
        if not token:
            print("[ERROR] No se pudo obtener token de Kashio")
            return False

        # Descargar datos
        data_count, final_token = await get_data_json_kashio_async(token, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[INFO] {data_count} registros descargados, generando archivo Excel")
            json_excel_kashio()
            print("[SUCCESS] Proceso Kashio completado exitosamente")
            return True
        else:
            print("[INFO] No hay datos para procesar")
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    print(f"[WRAPPER] Ejecutando Kashio collector")
    return asyncio.run(get_data_main_async(from_date, to_date))
