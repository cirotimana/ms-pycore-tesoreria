import asyncio
from playwright.async_api import async_playwright
import requests
from datetime import datetime, timedelta
from app.config import Config
from app.common.s3_utils import *
from app.digital.collectors.nuvei.get_qr_2mf.use_secret import main as get_validator_main
import re
import os



# =============================
#   CACHE DE SESION NUVEI
# =============================
class SessionCacheNuvei:
    def __init__(self):
        self.session_data = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_session(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.session_data and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando sesion Nuvei del cache")
                return self.session_data

            print("[INFO] Obteniendo nueva sesion Nuvei...")
            self.session_data = await get_nuvei_session()
            
            if self.session_data:
                # Sesion valida por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Sesion Nuvei cacheada hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nueva sesion Nuvei")
            
            return self.session_data

    def invalidate(self):
        print("[INFO] Invalidando sesion Nuvei cacheada")
        self.session_data = None
        self.expires_at = None


session_cache_nuvei = SessionCacheNuvei()


# =============================
#   OBTENER SESION NUVEI
# =============================
async def get_nuvei_session():
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Nuvei")
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
            
            context = await browser.new_context()
            page = await context.new_page()
            
            print("[INFO] Navegando a login de Nuvei")
            await page.goto("https://cpanel.nuvei.com/login", wait_until="networkidle", timeout=60000)
            
            # Login basico
            print("[INFO] Realizando login basico")
            await page.fill('input[name="username"]', Config.USER_NAME_NUVEI)
            await page.fill('input[name="password"]', Config.PASSWORD_NUVEI)
            await page.click('button[type="submit"]')
            
            # Esperar y procesar OTP
            print("[INFO] Esperando campo OTP")
            await page.wait_for_selector('#one_time_password', timeout=15000)
            
            validator = get_validator_main()
            validator_str = str(validator).strip()
            
            if len(validator_str) == 6 and validator_str.isdigit():
                print(f"[INFO] Ingresando codigo OTP: {validator_str}")
                await page.fill('#one_time_password', validator_str)
                await asyncio.sleep(0.5)
                await page.keyboard.press("Enter")
                print("[+] Codigo OTP ingresado correctamente")
            else:
                print(f"[ERROR] Codigo OTP invalido: {validator_str}")
                return None
            
            # Esperar que cargue la pagina despues del OTP
            print("[INFO] Esperando carga post-OTP")
            await page.wait_for_timeout(5000)
            
            # Capturar cookies de sesion
            print("[INFO] Capturando cookies y CSRF token")
            cookies = await context.cookies()
            
            # Capturar CSRF token
            csrf_token = await page.evaluate('''() => {
                const meta = document.querySelector('meta[name="csrf-token"]');
                return meta ? meta.content : null;
            }''')
            
            if not csrf_token:
                print("[ERROR] No se pudo obtener CSRF token")
                return None
            
            print(f"[SUCCESS] Sesion Nuvei obtenida - CSRF: {csrf_token[:20]}...")
            
            return {
                'csrf_token': csrf_token,
                'cookies': {c['name']: c['value'] for c in cookies}
            }
            
    except Exception as e:
        print(f"[ERROR] Error general en get_nuvei_session: {e}")
        return None
        
    finally:
        # Cierre GARANTIZADO de recursos
        await close_playwright_resources(browser, context, page, "Nuvei")
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass



async def close_playwright_resources(browser, context, page, resource_name=""):
    print(f"[INFO] Cerrando recursos de {resource_name}...")
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
            print(f"[DEBUG] Recursos de {resource_name} cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de {resource_name}: {e}")


# =============================
#   FUNCIONES DE EXPORTACION
# =============================
def is_valid_excel(content):
    if len(content) < 10000:
        return False
    
    # Verificar que no sea HTML
    if content[:100].lower().find(b'<!doctype html') != -1 or content[:100].lower().find(b'<html') != -1:
        return False
    
    # Verificar firmas de Excel
    excel_signatures = [
        b'\x50\x4b',  # ZIP (xlsx)
        b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1',  # OLE (xls)
    ]
    
    return any(content[:8].startswith(sig) for sig in excel_signatures)


async def send_export_request(csrf_token, session_cookies, from_date, to_date):
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-CSRF-TOKEN": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://cpanel.nuvei.com",
        "Referer": "https://cpanel.nuvei.com/operations/transactions",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Cookie": "; ".join([f"{k}={v}" for k, v in session_cookies.items()])
    }

    url = "https://cpanel.nuvei.com/report/operations/transactions-graphql/run"
    
    date_from = from_date.strftime("%Y-%m-%d 00:00:00")
    date_to = to_date.strftime("%Y-%m-%d 23:59:59")
    
    payload = {
        "_token": csrf_token,
        "is_export": "1",
        "export_type": "excel",
        "dateFrom": date_from,
        "dateTo": date_to,
        "ClientID[]": "251000780",
        "siteName": "244587",
        "transactionType": "1007,1004,1000,1006,1008,1017",
        "dateRange": "custom",
        "dateRangePicker": f"{from_date.strftime('%d %b, %Y')} 00:00 - {to_date.strftime('%d %b, %Y')} 23:59",
        "reportSlug": "transaction-search-graphql",
        "reportId": "729",
        "pageSize": "25",
        "page": "1",
    }

    columns = [
        "transactionDate", "transactionId", "paymentMethod", "subMethod", "transactionType",
        "authorizationType", "currency", "amount", "transactionResult", "reasonCode",
        "filterReason", "pan", "processingChannel", "acquirerBank", "ClientID", "siteName",
        "apmReference", "email", "pppOrderID", "clientUniqueID", "LCID",
        "externalSchemeIdentifier", "SchemeIdentifier", "upoid", "APMAdditionalInformationok",
        "RefundAuthorizationStatus", "externalTokenProvider", "issuerBank", "BIN", "nameOnCard", "cardType"
    ]
    
    for idx, col in enumerate(columns):
        payload[f"columnsOrder[{idx}]"] = col

    try:
        print(f"[INFO] Enviando solicitud de exportacion para {from_date.strftime('%d/%m/%Y')}")
        response = requests.post(url, headers=headers, data=payload, timeout=60000)
        
        if response.status_code == 200:
            # Buscar el path del archivo en la respuesta
            response_text = response.text
            path_match = re.search(r'Path: ([^|]+)', response_text)
            if path_match:
                file_path = path_match.group(1).strip()
                print(f"[SUCCESS] Solicitud de exportacion enviada exitosamente")
                print(f"[DEBUG] Archivo generado en: {file_path}")
                return True, file_path
            else:
                print("[SUCCESS] Solicitud enviada pero no se encontró path del archivo")
                return True, None
        elif response.status_code == 419:
            print("[WARN] Sesion expirada (419) - Invalidando cache")
            session_cache_nuvei.invalidate()
            return False, 419
        elif response.status_code == 401:
            print("[WARN] No autorizado (401) - Invalidando cache")
            session_cache_nuvei.invalidate()
            return False, 401
        else:
            print(f"[ERROR] Status code {response.status_code}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            return False, response.status_code
            
    except Exception as e:
        print(f"[ERROR] Error al enviar solicitud: {e}")
        return False, None


def try_download_excel(session_cookies, attempt, file_path=None):
    if file_path:
        # Usar el path específico del archivo
        filename = os.path.basename(file_path)
        url = f"https://cpanel.nuvei.com/exporter/export_download?file={filename}"
    else:
        # Fallback al método original
        url = "https://cpanel.nuvei.com/exporter/export_download"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://cpanel.nuvei.com/operations/transactions",
        "Cookie": "; ".join([f"{k}={v}" for k, v in session_cookies.items()])
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=60000)
        
        if response.status_code != 200:
            print(f"[WARN] Intento {attempt}: Status {response.status_code}")
            return None
        
        content_type = response.headers.get("content-type", "")
        
        # Si es HTML el archivo no esta listo
        if "text/html" in content_type:
            print(f"[WARN] Intento {attempt}: Archivo no listo aun (HTML recibido)")
            return None
        
        # Verificar si es Excel valido
        if not is_valid_excel(response.content):
            print(f"[WARN] Intento {attempt}: Contenido no es Excel valido")
            return None
        
        # Generar nombre de archivo
        cd = response.headers.get("content-disposition", "")
        m = re.search(r'filename=([^;]+)', cd)
        filename = m.group(1).strip('"') if m else f"nuvei_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
        
        # Subir a S3
        s3_key = f"digital/collectors/nuvei/input/{filename}"
        upload_file_to_s3(response.content, s3_key)
        
        print(f"[SUCCESS] Archivo guardado en S3: {s3_key}")
        return filename
        
    except Exception as e:
        print(f"[ERROR] Intento {attempt}: {e}")
        return None


async def download_with_retry(session_cookies, file_path=None, max_attempts=30):
    if file_path:
        print(f"[INFO] Esperando 5 segundos para que se genere el archivo...")
        await asyncio.sleep(5)
    else:
        print(f"[INFO] Esperando 30 segundos para que se genere el archivo...")
        await asyncio.sleep(30)
    
    for attempt in range(1, max_attempts + 1):
        print(f"[INFO] Intento de descarga {attempt}/{max_attempts}")
        
        filename = try_download_excel(session_cookies, attempt, file_path)
        
        if filename:
            print(f"[SUCCESS] Archivo descargado: {filename}")
            return filename
        
        if attempt < max_attempts:
            wait_time = 5 if file_path else 10
            print(f"[INFO] Esperando {wait_time} segundos antes del siguiente intento...")
            await asyncio.sleep(wait_time)
    
    print(f"[ERROR] No se pudo descargar el archivo despues de {max_attempts} intentos")
    return None


# =============================
#   FUNCIONES PRINCIPALES 
# =============================
async def process_day_download(from_date, to_date, max_retries=5):
    print(f"[INFO] Procesando fecha: {from_date.strftime('%d/%m/%Y')}")
    
    session_data = None
    
    for retry in range(max_retries):
        # Obtener sesion del cache (forzar refresh despues del primer intento fallido)
        force_refresh = (retry > 0)
        print(f"[INFO] Obteniendo sesion (intento {retry + 1}/{max_retries}, refresh: {force_refresh})")
        
        session_data = await session_cache_nuvei.get_session(force_refresh=force_refresh)
        
        if not session_data:
            print("[ERROR] No se pudo obtener sesion")
            if retry < max_retries - 1:
                print("[INFO] Esperando 5 segundos antes de reintentar...")
                await asyncio.sleep(5)
                continue
            return None
        
        csrf_token = session_data.get('csrf_token')
        session_cookies = session_data.get('cookies')
        
        if not csrf_token or not session_cookies:
            print("[ERROR] Sesion incompleta - faltan CSRF token o cookies")
            session_cache_nuvei.invalidate()
            if retry < max_retries - 1:
                await asyncio.sleep(5)
                continue
            return None
        
        # Enviar solicitud de exportacion
        success, result = await send_export_request(csrf_token, session_cookies, from_date, to_date)
        
        if isinstance(result, int) and result in [419, 401]:
            print(f"[WARN] Sesion expirada ({result}), obteniendo nueva sesion")
            session_cache_nuvei.invalidate()
            if retry < max_retries - 1:
                continue
            return None
        
        if not success:
            print("[ERROR] No se pudo enviar la solicitud de exportacion")
            if retry < max_retries - 1:
                await asyncio.sleep(5)
                continue
            return None
        
        # Intentar descargar el archivo (result puede ser el file_path)
        file_path = result if isinstance(result, str) else None
        filename = await download_with_retry(session_cookies, file_path)
        
        if filename:
            return filename
        
        # Si fallo la descarga, reintentar con nueva sesion
        print(f"[WARN] Descarga fallida, reintentando con nueva sesion ({retry + 2}/{max_retries})")
        session_cache_nuvei.invalidate()
    
    return None


async def get_main_download(from_date, to_date):
    start_date = from_date.replace(hour=0, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    
    current = start_date
    downloaded_files = []
    
    print(f"[INICIO] Procesando descargas Nuvei desde {start_date.strftime('%d/%m/%Y')} hasta {end_date.strftime('%d/%m/%Y')}")
    
    while current < end_date:
        from_d = current
        to_d = current
        
        print(f"[INFO] Procesando dia: {from_d.strftime('%d/%m/%Y')}")
        
        try:        
            filename = await process_day_download(from_d, to_d)
            
            if filename:
                print(f"[SUCCESS] Descarga completada: {filename}")
                downloaded_files.append(filename)
            else:
                print(f"[ERROR] No se pudo completar la descarga para {from_d.strftime('%d/%m/%Y')}")
                # Continuar con el siguiente dia en lugar de retornar None
                print("[INFO] Continuando con el siguiente dia...")

        except Exception as e:
            print(f"[ERROR] Error procesando dia {from_d.strftime('%d/%m/%Y')}: {e}")
            print("[INFO] Continuando con el siguiente dia...")
        
        current += timedelta(days=1)
        
        # Pequeña pausa entre dias
        if current < end_date:
            print("[INFO] Esperando 2 segundos antes del siguiente dia...")
            await asyncio.sleep(2)
        
    if downloaded_files:
        print(f"[SUCCESS] Procesamiento completado. Total de archivos descargados: {len(downloaded_files)}")
        return downloaded_files
    else:
        print("[ERROR] No se pudo descargar ningun archivo")
        return None


