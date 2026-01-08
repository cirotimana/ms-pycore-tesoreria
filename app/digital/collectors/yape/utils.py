import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import requests
from datetime import datetime, timedelta
import json
import pytz
from app.config import Config
from io import BytesIO
import time
from app.common.s3_utils import *
import math
import zipfile
import os


# =============================
#   CACHE DE TOKEN YAPE
# =============================
class TokenCacheYape:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False, type = 2):
        async with self.lock:
            now = datetime.now()

            if (not force_refresh and self.token and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token Yape del cache")
                return self.token

            print(f"[INFO] Obteniendo nuevo token Yape tipo {type}...")
            if type == 1:
                self.token = await get_token_yape_1()
            else:
                self.token = await get_token_yape_2()

            if self.token:
                # Token valido por 30 minutos (menos que la sesion real)
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token cacheado hasta {self.expires_at}")

            return self.token

    def invalidate(self):
        self.token = None
        self.expires_at = None


token_cache_yape = TokenCacheYape()



# =============================
#      NUEVO PROCESO
# =============================

# paso 1 obtenemos token##
async def get_token_yape_2(max_login_attempts=5):
    print("[INFO] Iniciando Playwright para obtener token Yape")
    
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Yape")
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )

            print("[INFO] Inyectando script para ocultar webdriver")
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()
            token_found = None

            async def handle_request(route, request):
                nonlocal token_found
                headers = request.headers
                if 'authorization' in headers and not token_found:
                    token_found = headers['authorization']
                    print(f"[DEBUG] Token detectado: {token_found[:20]}...")
                await route.continue_()

            print("[INFO] Interceptando todas las requests para buscar el token")
            await context.route("**", handle_request)

            print("[INFO] Navegando a https://www.niubizenlinea.com.pe/")
            try:
                await page.goto("https://www.niubizenlinea.com.pe/", wait_until="networkidle", timeout=60000)
                print("[INFO] Pagina cargada correctamente")
            except Exception as e:
                print(f"[ERROR] Error al cargar la pagina: {e}")
                return None

            async def intentar_login_yape():
                try:
                    print("[DEBUG] Esperando que los campos esten visibles...")
                    await asyncio.sleep(3)
                    
                    # Cerrar modal de cookies si existe
                    try:
                        cookie_modal = await page.query_selector('#CybotCookiebotDialog')
                        if cookie_modal:
                            print("[DEBUG] Modal de cookies detectado, intentando cerrar...")
                            
                            # Intentar hacer clic en "Aceptar todas"
                            cookie_buttons = [
                                '#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll',
                                'button:has-text("Aceptar todas")',
                                '#CybotCookiebotDialogBodyButtonAccept',
                                'button:has-text("Aceptar")',
                                '.CybotCookiebotDialogBodyButton'
                            ]
                            
                            cookie_closed = False
                            for cookie_selector in cookie_buttons:
                                try:
                                    await page.click(cookie_selector, timeout=2000)
                                    print(f"[SUCCESS] Modal cerrado con: {cookie_selector}")
                                    cookie_closed = True
                                    break
                                except:
                                    continue
                            
                            if not cookie_closed:
                                # Si no se pudo hacer clic, ocultar con JavaScript
                                await page.evaluate('document.getElementById("CybotCookiebotDialog").style.display = "none"')
                                print("[INFO] Modal ocultado con JavaScript")
                            
                            await asyncio.sleep(1)
                    except Exception as e:
                        print(f"[DEBUG] Error manejando modal de cookies: {e}")
                    
                    username_selectors = [
                        'input[id="txtEmail"]',
                        'input[placeholder="Ingresa tu correo electrónico"]',
                        'input[type="text"].vn-input'
                    ]
                    
                    password_selectors = [
                        'input[placeholder="Ingresa tu contraseña"]',
                        'input[type="password"].vn-input'
                    ]
                    
                    button_selectors = [
                        'button[type="button"].button--primary',
                        'button.button--primary',
                        'button:has-text("Iniciar sesión")'
                    ]
                    
                    # Llenar username
                    username_filled = False
                    for selector in username_selectors:
                        try:
                            print(f"[DEBUG] Intentando llenar username con selector: {selector}")
                            await page.wait_for_selector(selector, state='visible', timeout=5000)
                            await page.fill(selector, Config.USER_NAME_YAPE)
                            print(f"[SUCCESS] Username llenado con: {selector}")
                            username_filled = True
                            break
                        except Exception as e:
                            print(f"[DEBUG] Selector {selector} fallo: {e}")
                            continue
                    
                    if not username_filled:
                        print("[ERROR] No se pudo llenar el campo de usuario")
                        return False
                    
                    await asyncio.sleep(0.5)
                    
                    # Llenar password
                    password_filled = False
                    for selector in password_selectors:
                        try:
                            print(f"[DEBUG] Intentando llenar password con selector: {selector}")
                            await page.wait_for_selector(selector, state='visible', timeout=5000)
                            await page.fill(selector, Config.PASSWORD_YAPE)
                            print(f"[SUCCESS] Password llenado con: {selector}")
                            password_filled = True
                            break
                        except Exception as e:
                            print(f"[DEBUG] Selector {selector} fallo: {e}")
                            continue
                    
                    if not password_filled:
                        print("[ERROR] No se pudo llenar el campo de contraseña")
                        return False
                    
                    await asyncio.sleep(0.5)
                    
                    # Click en boton
                    button_clicked = False
                    for selector in button_selectors:
                        try:
                            print(f"[DEBUG] Intentando click con selector: {selector}")
                            await page.wait_for_selector(selector, state='visible', timeout=5000)
                            await page.click(selector)
                            print(f"[SUCCESS] Click realizado con: {selector}")
                            button_clicked = True
                            break
                        except Exception as e:
                            print(f"[DEBUG] Selector {selector} fallo: {e}")
                            continue
                    
                    if not button_clicked:
                        print("[ERROR] No se pudo hacer click en el boton de login")
                        return False
                    
                    return True
                    
                except Exception as e:
                    print(f"[ERROR] Error en intentar_login_yape: {e}")
                    return False

            login_attempt = 0
            while login_attempt < max_login_attempts:
                print(f"[INFO] Intento de login #{login_attempt + 1}")
                
                login_success = await intentar_login_yape()
                
                if not login_success:
                    print(f"[WARN] Fallo el intento de login #{login_attempt + 1}")
                    login_attempt += 1
                    if login_attempt < max_login_attempts:
                        print("[INFO] Refrescando pagina para nuevo intento...")
                        await page.reload(wait_until="networkidle", timeout=30000)
                        await asyncio.sleep(2)
                    continue

                # CAMBIO CLAVE: Esperar navegacion o cambio de URL despues del login
                print("[INFO] Esperando navegacion post-login...")
                try:
                    # Esperar que la URL cambie o que se cargue una nueva pagina
                    await page.wait_for_load_state("networkidle", timeout=10000)
                    print("[SUCCESS] Navegacion post-login completada")
                except Exception as e:
                    print(f"[DEBUG] No hubo navegacion inmediata: {e}")

                # Esperar captura del token con timeout mas corto inicialmente
                print("[INFO] Esperando captura del token (15s inicial)")
                token_capturado = False
                
                # Primer intento: 15 segundos (deberia ser suficiente)
                for i in range(15):
                    if token_found:
                        print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                        token_capturado = True
                        break
                    await asyncio.sleep(1)

                if token_capturado:
                    break

                # Si no se capturo, dar 15 segundos mas antes de reintentar
                print("[INFO] Extendiendo espera del token (15s adicionales)...")
                for i in range(15, 30):
                    if token_found:
                        print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                        token_capturado = True
                        break
                    await asyncio.sleep(1)

                if token_capturado:
                    break

                # Si aun no hay token, intentar navegar manualmente a una ruta que dispare el token
                print("[WARN] Token no detectado, intentando forzar peticiones...")
                try:
                    # Navegar a una ruta interna que requiera autenticacion
                    current_url = page.url
                    if "login" in current_url or "comercio.niubiz.com.pe" == current_url:
                        print("[DEBUG] Intentando navegar a dashboard...")
                        await page.goto("https://comercio.niubiz.com.pe/dashboard", wait_until="networkidle", timeout=15000)
                        
                        # Esperar 10 segundos mas
                        for i in range(10):
                            if token_found:
                                print(f"[SUCCESS] Token capturado despues de navegacion en segundo {i + 1}")
                                token_capturado = True
                                break
                            await asyncio.sleep(1)
                except Exception as e:
                    print(f"[DEBUG] No se pudo navegar a dashboard: {e}")

                if token_capturado:
                    break

                # Si todavia no hay token, refrescar y volver a intentar
                print(f"[WARN] Token no detectado en intento #{login_attempt + 1}, refrescando pagina")
                await page.reload(wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)
                login_attempt += 1

            if token_found:
                print("[INFO] Token capturado exitosamente")
                return token_found
            else:
                print(f"[ERROR] No se encontro token despues de {max_login_attempts} intentos")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_yape: {e}")
        return None
        
    finally:
        await close_playwright_resources_yape(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass
        
        
async def close_playwright_resources_yape(browser, context, page):
    print("[INFO] Cerrando recursos de Yape...")
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
            print("[DEBUG] Recursos de Yape cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de Yape: {e}")
        

# paso 2 hacemos el filtro por fecha de la data para generar reporte 
async def get_download_data_yape_async(token, from_date, to_date):
    
    start_date = from_date.replace(hour=0, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)

    print(f"[INFO] Descargando transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Authorization": token,
        "Origin": "https://www.niubizenlinea.com.pe",
        "Referer": "https://www.niubizenlinea.com.pe/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }

    url = "https://api.niubizenlinea.com.pe/ms-download/api/downloads/reports/sales/scheduling"

    current = start_date
    current_token = token
    id_list = []  # Lista para almacenar todos los IDs
    
    while current < end_date:
        current_date_str = current.strftime("%Y%m%d") 
        print(f"[INFO] Descargando transacciones del {current_date_str}")
        
        body = {
            "documentType": "CSV",
            "ruc": ["20563314924"],
            "commerceCode": ["651000585"],  # CORREGIDO: usar -1 como en el payload real
            "currencyCode": "604",
            "startDate": current_date_str,
            "endDate": current_date_str,
            "codMarcaTarjeta": "-1",
            "statusCode": ["-1"]
        }
        
        max_retries = 5
        retry_count = 0
        request_success = False
        
        while retry_count < max_retries and not request_success:
            try:
                response = requests.post(url, headers=headers, json=body, timeout=240)
                
                if response.status_code == 201:
                    data = response.json()
                    
                    if isinstance(data, dict) and "statusCode" in data and data["statusCode"] == 201:
                        if "data" in data:
                            id_value = data["data"]
                            id_list.append(id_value)
                            print(f"[SUCCESS] ID obtenido para {current_date_str}: {id_value}")
                            request_success = True
                        else:
                            print(f"[WARN] No se encontro 'data' en la respuesta para {current_date_str}")
                            request_success = True  # Continuar con el siguiente día
                    else:
                        print(f"[WARN] StatusCode no es 201 para {current_date_str}: {data}")
                        request_success = True  # Continuar con el siguiente día
                        
                elif response.status_code == 401:
                    print(f"[ERROR] Error 401 para {current_date_str}")
                    
                    if retry_count < max_retries - 1:
                        print("[INFO] Renovando token...")
                        new_token = await token_cache_yape.get_token(force_refresh=True)
                        if new_token:
                            current_token = new_token
                            headers["Authorization"] = current_token
                            print("[INFO] Token renovado")
                        else:
                            print("[ERROR] No se pudo renovar el token")
                            request_success = True  # Salir del bucle de reintentos
                    else:
                        print("[ERROR] Maximos reintentos alcanzados")
                        request_success = True
                        
                else:
                    print(f"[ERROR] Error {response.status_code} para {current_date_str}: {response.text}")
                    request_success = True  # Continuar con el siguiente día
                    
            except Exception as e:
                print(f"[ERROR] Excepcion para {current_date_str}: {e}")
                
            retry_count += 1
                
        print("[INFO] Esperando 10 seg para la siguiente solicitud")
        await asyncio.sleep(10)

        current += timedelta(days=1)
    
    print(f"[DEBUG] IDs obtenidos: {id_list}")
    return id_list, current_token


# paso 3: verificar estado de los reportes
async def check_download_status_async(token, user_id="68acfb6c8029a31646a25391"):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Authorization": token,
        "Origin": "https://www.niubizenlinea.com.pe",
        "Referer": "https://www.niubizenlinea.com.pe/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }
    
    url = "https://api.niubizenlinea.com.pe/ms-download/api/downloads/filters"
    
    body = {
        "userId": user_id,
        "name": ""
    }
    
    try:
        response = requests.post(url, headers=headers, json=body, timeout=240)
        
        if response.status_code == 201:
            data = response.json()
            
            if "downloadsList" in data:
                completed_downloads = []
                for download in data["downloadsList"]:
                    if download.get("downloadStatus", {}).get("description") == "Terminado":
                        completed_downloads.append({
                            "id": download["_id"],
                            "filename": download["filename"],
                            "status": download["downloadStatus"]["description"]
                        })
                        print(f"[INFO] Archivo listo: {download['_id']} - {download['filename']}")
                
                return completed_downloads
            else:
                print("[WARN] No se encontro 'downloadsList' en la respuesta")
                return []
        else:
            print(f"[ERROR] Error {response.status_code}: {response.text}")
            return []
            
    except Exception as e:
        print(f"[ERROR] Error verificando estado: {e}")
        return []


def process_zip_file_yape(zip_content, timestamp):
    downloaded_files = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
            file_list = zip_file.namelist()
            print(f"[INFO] Archivos en ZIP: {file_list}")
            
            for file_name in file_list:
                if file_name.endswith(('.csv', '.xlsx', '.xls')):
                    print(f"[INFO] Extrayendo: {file_name}")
                    
                    with zip_file.open(file_name) as extracted_file:
                        file_content = extracted_file.read()
                    
                    base_name = os.path.splitext(file_name)[0]
                    file_ext = os.path.splitext(file_name)[1]
                    output_key = f"digital/collectors/yape/input/{base_name}_{timestamp}{file_ext}"
                    
                    # Subir a S3
                    with BytesIO(file_content) as buffer:
                        upload_file_to_s3(buffer.getvalue(), output_key)
                    
                    # Descargar a local
                    # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
                    
                    downloaded_files.append(output_key)
                    print(f"[SUCCESS] Archivo extraido y guardado: {output_key}")
                    
    except Exception as e:
        print(f"[ERROR] Error al procesar ZIP: {e}")
    
    return downloaded_files


# paso 4: descargar archivos completados
async def download_files_async(token, download_ids):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": token,
        "Origin": "https://www.niubizenlinea.com.pe",
        "Referer": "https://www.niubizenlinea.com.pe/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    }
    
    downloaded_files = []
    
    for download_info in download_ids:
        download_id = download_info["id"]
        filename = download_info["filename"]
        
        url = f"https://api.niubizenlinea.com.pe/ms-download/api/downloads/file/{download_id}"
        
        try:
            print(f"[INFO] Descargando archivo: {filename}")
            response = requests.get(url, headers=headers, timeout=240)
            
            if response.status_code == 200:
                content = response.content
                current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
                
                if filename.endswith('.zip'):
                    print("[INFO] Archivo ZIP detectado, descomprimiendo...")
                    extracted_files = process_zip_file_yape(content, current_time)
                    downloaded_files.extend(extracted_files)
                else:
                    # Si no es ZIP, guardar directamente
                    output_key = f"digital/collectors/yape/input/{filename.replace('.zip', '')}_{current_time}"
                    
                    with BytesIO(content) as buffer:
                        upload_file_to_s3(buffer.getvalue(), output_key)
                    
                    # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
                    downloaded_files.append(output_key)
                    print(f"[SUCCESS] Archivo guardado: {output_key}")
                
            else:
                print(f"[ERROR] Error {response.status_code} descargando {download_id}: {response.text}")
                
        except Exception as e:
            print(f"[ERROR] Error descargando {download_id}: {e}")
    
    return downloaded_files


# funcion que combina todo el proceso
async def get_yape_reports_async(token, from_date, to_date, max_wait_minutes=30):
    print("[INFO] Iniciando proceso completo de descarga de reportes Yape")
    
    # Paso 1: Solicitar reportes
    id_list, updated_token = await get_download_data_yape_async(token, from_date, to_date)
    
    if not id_list:
        print("[WARN] No se obtuvieron IDs para procesar")
        return []
    
    print(f"[INFO] Esperando que se procesen {len(id_list)} reportes...")
    
    # Paso 2: Esperar y verificar estado
    max_attempts = max_wait_minutes * 2  # verificar cada 30 segundos
    
    for attempt in range(max_attempts):
        print(f"[INFO] Verificando estado (intento {attempt + 1}/{max_attempts})")
        
        completed_downloads = await check_download_status_async(updated_token)
        
        # Filtrar solo los IDs que solicitamos
        ready_downloads = [d for d in completed_downloads if d["id"] in id_list]
        
        if len(ready_downloads) == len(id_list):
            print(f"[SUCCESS] Todos los reportes están listos ({len(ready_downloads)})")
            
            # Paso 3: Descargar archivos
            downloaded_files = await download_files_async(updated_token, ready_downloads)
            return downloaded_files
        
        elif ready_downloads:
            print(f"[INFO] {len(ready_downloads)}/{len(id_list)} reportes listos")
        
        if attempt < max_attempts - 1:
            print("[INFO] Esperando 30 segundos antes del siguiente chequeo...")
            await asyncio.sleep(30)
    
    print(f"[WARN] Timeout después de {max_wait_minutes} minutos")
    
    # Descargar los que estén listos
    if ready_downloads:
        print(f"[INFO] Descargando {len(ready_downloads)} archivos disponibles")
        downloaded_files = await download_files_async(updated_token, ready_downloads)
        return downloaded_files
    
    return []


# =============================
#      FUNCION PRINCIPAL
# =============================
async def get_data_main_async_2(from_date, to_date):
    try:
        print("[INFO] Iniciando captura de token Yape")
        token = await token_cache_yape.get_token()
        
        if token:
            print("[INFO] Trayendo datos de Yape")
            downloaded_files = await get_yape_reports_async(token, from_date, to_date)
            print(f"Archivos descargados: {downloaded_files}")
            return downloaded_files
        else:
            print("[ALERTA] No se pudo obtener el token de Yape.")
            
    except Exception as e:
        print(f"[✖] Error en obtener la data de Yape: {e}")
    

def get_data_main_2(from_date, to_date):
    print(f"[INICIO] Ejecutando Yape para {from_date} a {to_date}")
    result = asyncio.run(get_data_main_async_2(from_date, to_date))
    print(f"[FIN] Proceso Yape completado {result}")
    return result


### OTRAS ###
def save_dfs_to_excel(writer, dfs_dict, chunk_size=1_000_000):
    for sheet_name, df in dfs_dict.items():
        num_chunks = math.ceil(len(df) / chunk_size) if len(df) > 0 else 1
        for i in range(num_chunks):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            chunk = df.iloc[start:end]
            final_sheet_name = f"{sheet_name}_{i+1}" if num_chunks > 1 else sheet_name
            chunk.to_excel(writer, sheet_name=final_sheet_name, index=False)



# =============================
#   OPCION JSON 
# =============================
async def get_token_yape_1(max_login_attempts=3):
    print("[INFO] Iniciando Playwright para obtener token Yape")
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador Chrome en modo headless")
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )

            print("[INFO] Inyectando script para ocultar webdriver")
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()
            token_found = None

            async def handle_request(route, request):
                nonlocal token_found
                headers = request.headers
                if 'authorization' in headers and not token_found:
                    token_found = headers['authorization']
                    print(f"[DEBUG] Token detectado: {token_found[:20]}...")
                await route.continue_()

            print("[INFO] Interceptando todas las requests para buscar el token")
            await context.route("**", handle_request)

            print("[INFO] Navegando a https://comercio.niubiz.com.pe")
            try:
                await page.goto("https://comercio.niubiz.com.pe", wait_until="networkidle", timeout=60000)
                print("[INFO] Pagina cargada correctamente")
            except Exception as e:
                print(f"[ERROR] Error al cargar la pagina: {e}")
                return None
            
            login_attempt = 0
            while login_attempt < max_login_attempts:
                print(f"[INFO] Intento de login #{login_attempt + 1}")
                
                # Login (manteniendo la logica original que funciona)
                try:
                    await page.fill('input[name="username"], input[type="email"], input[type="text"]', Config.USER_NAME_YAPE)
                    await page.fill('input[name="password"], input[type="password"]', Config.PASSWORD_YAPE)
                    print("[INFO] Haciendo click en el boton de login")
                    await page.click('button[type="submit"], #kc-login')
                except Exception as e:
                    print(f"[ERROR] Error durante el login: {e}")
                    break
                
                # Esperar captura del token
                print("[INFO] Esperando captura del token (30s max)")
                token_capturado = False
                for i in range(30):
                    if token_found:
                        print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                        token_capturado = True
                        break
                    await asyncio.sleep(1)
                    
                if token_capturado:
                    break
                
                # Si no se obtuvo el token, refrescar y volver a intentar
                print("[INFO] Token no detectado, refrescando pagina")
                await page.reload(wait_until="networkidle")
                login_attempt += 1
                
            if token_found:
                print("[INFO] Token capturado exitosamente")
                return token_found
            else:
                print("[ERROR] No se encontro token despues de {max_login_attempts} intentos")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_yape: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_yape(browser, context, page)




async def get_data_json_yape_async(token, from_date, to_date):
    
    start_date = from_date.replace(hour=0, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)

    print(f"[INFO] Descargando transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }
    url = "https://api.niubiz.pe/api.backoffice.merchant/order"
    size = 10000  
    all_data = []
    current_token = token

    current = start_date
    while current < end_date:
        start_time = time.time()
        from_date_str = current.strftime("%d/%m/%Y")
        to_date_str = current.strftime("%d/%m/%Y")
        print(f"[INFO] Descargando transacciones del {from_date_str}")

        page = 1
        day_data = []  
        has_more_pages = True
        last_record_ids = set()  # Guardar IDs reales para detectar duplicados
        
        while has_more_pages:
            body = {
                "page": page,
                "size": size,
                "fromDate": from_date_str,
                "toDate": to_date_str,
                "merchantId": "651000585",
                "brand": [],
                "status": [],
                "currency": [],
                "transactionDate": "",
                "confirmationDate": "",
                "pagolinkId": None,
                "export": "false"
            }

            max_retries = 5
            records = []
            request_success = False
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(url, headers=headers, json=body, timeout=240)
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, dict) and "data" in data and "list" in data["data"]:
                            records = data['data']['list']
                        else:
                            records = data if isinstance(data, list) else []

                        if not records:
                            print(f"[ALERTA] No hay registros para {from_date_str}, pagina {page}")
                            has_more_pages = False
                            request_success = True
                            break

                        # Extraer IDs únicos de esta página
                        current_ids = {record.get('id') or record.get('purchaseNumber') for record in records if isinstance(record, dict)}
                        
                        # Si todos los IDs ya existen, son verdaderos duplicados
                        if current_ids and current_ids.issubset(last_record_ids):
                            print(f"[WARN] Registros duplicados detectados en pagina {page}. Deteniendo.")
                            has_more_pages = False
                            request_success = True
                            break
                        
                        last_record_ids.update(current_ids)
                        day_data.extend(records)
                        print(f"[INFO] Descargados {len(records)} registros (pagina {page})")
                        if len(records) < size:
                            has_more_pages = False
                        else:
                            page += 1
                        
                        request_success = True
                        break
                        
                    elif response.status_code == 401:
                        print(f"[ERROR] Error de autorizacion (401) en pagina {page}. Intento {attempt + 1}/{max_retries}")
                        if attempt < max_retries - 1:
                            print("[INFO] Intentando renovar token Yape...")
                            # CORREGIDO: await en lugar de asyncio.run()
                            new_token = await token_cache_yape.get_token(force_refresh=True)
                            if new_token:
                                current_token = new_token
                                headers["Authorization"] = current_token
                                print("[INFO] Token Yape renovado correctamente.")
                            else:
                                print("[ALERTA] No se pudo renovar el token Yape.")
                                await asyncio.sleep(5)  
                        else:
                            print(f"[ERROR] Fallo por error de autorizacion en pagina {page}")
                            has_more_pages = False
                            request_success = True
                            
                    elif response.status_code == 504:
                        print(f"[ERROR] Timeout (504) en pagina {page}. Intento {attempt + 1}/{max_retries}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(5)  
                        else:
                            print(f"[ERROR] Fallo por timeout en pagina {page}")
                            has_more_pages = False
                            request_success = True
                    else:
                        print(f"[ERROR] {response.status_code}: {response.text}")
                        has_more_pages = False
                        request_success = True
                        break
                except Exception as e:
                    print(f"[ERROR] Excepcion en pagina {page}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)  
                    else:
                        has_more_pages = False
                        request_success = True
                        break
            
            if not request_success:
                print(f"[WARN] No se procesó la página {page} correctamente")
            
            await asyncio.sleep(1) 
        
        end_time = time.time()
        print(f"[INFO] Total registros para {from_date_str}: {len(day_data)}")
        print(f"[INFO] Tiempo total de ejecucion para {from_date_str}: {end_time - start_time:.2f} segundos")

        all_data.extend(day_data)
        
        current += timedelta(days=1)
    
    if all_data:
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        file_key = f"digital/collectors/yape/input/response_{current_time}.json"

        upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
        print(f"[✔] Archivo guardado en S3: {file_key} con {len(all_data)} registros")
        ##download_file_from_s3_to_local(file_key)  # pruebitas
    return len(all_data)



async def get_data_main_json_async(from_date, to_date):
    try:
        print("[INFO] Iniciando captura de token Yape _ json")
        # CORREGIDO: await directo
        token = await token_cache_yape.get_token(type=1)
        
        if token:
            print("[INFO] Trayendo datos de Yape _ json")
            # CORREGIDO: await directo
            data_count = await get_data_json_yape_async(token, from_date, to_date)
            print(f"[DEBUG] Datos obtenidos: {data_count}")
            
            if data_count > 0:
                print("[INFO] Generando archivo Excel _ del json")
                processed_files = json_excel_yape()  
                return processed_files 
            else:
                print("[ALERTA] No hay datos para procesar en json.")
                return []  
        else:
            print("[ALERTA] No se pudo obtener el token de Yape en json.")
            return []  

    except Exception as e:
        print(f"[✖] Error en obtener la data de Yape json: {e}")
        return []


def get_data_main_json(from_date, to_date):
    return asyncio.run(get_data_main_json_async(from_date, to_date))



def json_excel_yape():
    prefix = "digital/collectors/yape/input/"
    files = list_files_in_s3(prefix)
    
    output_key  =""
    processed_files = []

    for file_key in files:
        if not file_key.endswith(".json") or "/input/processed/" in file_key:
            continue

        print(f"[INFO] Procesando {file_key}")
        content = read_file_from_s3(file_key)
        data = json.loads(content.decode("utf-8"))

        # Extraer la lista de registros
        if isinstance(data, dict) and "data" in data and "list" in data["data"]:
            records = data["data"]["list"]
        elif isinstance(data, list):
            records = data
        else:
            print("[ALERTA] No hay datos validos para procesar.")
            continue
        if not records or not isinstance(records, list):
            print("[ALERTA] No hay registros validos para procesar.")
            continue
        
        rows = []
        for item in records:
            row = {campo["label"]: campo["value"] for campo in item if "label" in campo and "value" in campo}
            rows.append(row)


        # Convertir a DataFrame y guardar como CSV
        df = pd.DataFrame(rows)
        
        df = df.rename(columns={'purchaseNumber':'Nro Pedido'})
        df = df.rename(columns={'clientName':'Cliente'})
        df = df.rename(columns={'currency':'Moneda'})
        df = df.rename(columns={'amount':'Importe Pedido'})
        df = df.rename(columns={'brand':'Marca'})
        df = df.rename(columns={'channelQr':'Canal QR'})
        df = df.rename(columns={'transactionDate':'Fecha de Transacción'})
        df['Fecha de Liquidación'] = ''
        df = df.rename(columns={'status':'Estado'})
        df['Código de transaccion'] = ''
        df = df.rename(columns={'confirmationDate':'Fecha de autorización'})


        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            output_key = file_key.replace(".json", ".csv")
            upload_file_to_s3(buffer.getvalue(), output_key)


        # Mover el .json a input/processed/
        processed_key = file_key.replace("input/", "input/processed/", 1)
        upload_file_to_s3(content, processed_key)
        delete_file_from_s3(file_key)
        
        processed_files.append(output_key)
        
        ##download_file_from_s3_to_local(output_key)##solo para pruebitas
        print(f"[INFO] Procesado: {file_key} -> {output_key} y movido a {processed_key}")

    print("[✔] Proceso Json -> Excel completado.")
    return processed_files 



if __name__ == "__main__":
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    print(f"[DEBUG] Enviando fechas from_date : {now} , to_date : {now}")
    result = get_data_main_json(now, now)
    print(f"[DEBUG] Resultados finales: {result}")
