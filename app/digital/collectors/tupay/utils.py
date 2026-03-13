import asyncio
from playwright.async_api import async_playwright
import requests
import threading
from datetime import datetime, timedelta
import pytz
from app.config import Config
import time
from app.common.s3_utils import *
import urllib3
import imaplib
import email
import re
import html
import subprocess
import tempfile
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =============================
#   CACHE DE SESION TUPAY
# =============================
class SessionCacheTupay:
    def __init__(self):
        self.bearer_cookie = None
        self.expires_at = None
        self.lock = threading.Lock()

    async def get_session(self, force_refresh=False):
        with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.bearer_cookie and 
                self.expires_at and now < self.expires_at):
                print("[info tupay] Usando sesion Tupay del cache")
                return self.bearer_cookie

            print("[info tupay] Obteniendo nueva sesion Tupay...")
            self.bearer_cookie = await get_token_tupay()
            
            if self.bearer_cookie:
                # Sesion valida por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[info tupay] Sesion Tupay cacheada hasta {self.expires_at}")
            else:
                print("[error tupay] No se pudo obtener nueva sesion Tupay")
            
            return self.bearer_cookie

    def invalidate(self):
        with self.lock:
            print("[info tupay] Invalidando sesion Tupay cacheada")
            self.bearer_cookie = None
            self.expires_at = None


session_cache_tupay = SessionCacheTupay()


# =============================
#   FUNCIONES DE UTILIDAD
# =============================
def date_to_timestamp(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())


# =============================
#   OBTENER TOKEN
# =============================
async def get_token_tupay():
    
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[info tupay] Lanzando navegador para Tupay")
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                ignore_https_errors=True
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()
            bearer_cookie = None

            # Escuchar peticiones de red para capturar el token
            async def handle_request(request):
                nonlocal bearer_cookie
                if bearer_cookie: return # ya lo capturamos
                
                # Buscamos el token en los headers de cualquier peticion saliente
                headers = request.headers
                cookie_str = headers.get("cookie", "")
                if "BEARER_TOKEN=" in cookie_str:
                    # Extraer el valor del token
                    parts = cookie_str.split(";")
                    for p in parts:
                        if "BEARER_TOKEN=" in p:
                            bearer_cookie = p.strip()
                            print(f"[ok tupay] Token capturado via red: {bearer_cookie[:25]}...")

            page.on("request", handle_request)

            try:
                print("[info tupay] Navegando a login de Tupay...")
                await page.goto("https://merchants.tupaypagos.com/login", wait_until="networkidle", timeout=90000)
                
                # Login
                print(f"[info tupay] Llenando credenciales para: {Config.USER_NAME_TUPAY}")
                await page.fill('input[name="email"], input[type="email"], input[type="text"]', Config.USER_NAME_TUPAY)
                await page.fill('input[name="password"], input[type="password"]', Config.PASSWORD_TUPAY)
                
                print("[info tupay] Click en submit e iniciando espera de redireccion...")
                await page.click('button[type="submit"]')
                
                # Esperar que la aplicacion cargue y se realicen peticiones con el token
                try:
                    await page.wait_for_url("**/home**", timeout=45000)
                    print("[ok tupay] Redireccion a /home exitosa")
                except:
                    print("[warn tupay] No se detecto redirección a /home, pero seguimos buscando el token...")

                # Darle unos segundos adicionales para capturar el token de las peticiones de fondo
                for _ in range(10):
                    if bearer_cookie:
                        break
                    await asyncio.sleep(1)

                if bearer_cookie:
                    print("[ok tupay] Proceso de captura completado exitosamente")
                    return bearer_cookie
                else:
                    # Intento de respaldo via cookies del contexto
                    print("[warn tupay] No se capturo token via red, intentando via cookies...")
                    cookies = await context.cookies()
                    cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                    
                    if 'BEARER_TOKEN=' in cookie_header:
                        for part in cookie_header.split(';'):
                            if part.strip().startswith('BEARER_TOKEN='):
                                bearer_cookie = part.replace(' ', '').strip()
                                print(f"[ok tupay] Token capturado via cookies: {bearer_cookie[:25]}...")
                                return bearer_cookie

                    print("[error tupay] No se pudo capturar el BEARER_TOKEN por ningún método")
                    return None

            except Exception as e:
                print(f"[error tupay] Error durante la captura: {e}")
                return None
                
    except Exception as e:
        print(f"[error tupay] Error general en get_token_tupay: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_tupay(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[debug] Playwright cerrado completamente.")
        except Exception:
            pass



async def close_playwright_resources_tupay(browser, context, page):
    print("[info tupay] Cerrando recursos de Tupay...")
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
            print("[debug] Recursos de Tupay cerrados correctamente")
        except Exception as e:
            print(f"[warn tupay] Error cerrando recursos de Tupay: {e}")


# =============================
#   FUNCIONES DE DATOS
# =============================
async def get_data_json_tupay_async(bearer_cookie, from_date, to_date):
    
    start_date = from_date.replace(hour=5, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=5, minute=0, second=0)
    
    from_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S") 
    to_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[info tupay] Descargando transacciones del: {from_date_str} al {to_date_str}")
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": bearer_cookie
    }
    
    url = "https://merchants-api.tupayonline.com/v1/deposits/export"
    
    from_timestamp = date_to_timestamp(from_date_str)
    to_timestamp = date_to_timestamp(to_date_str)

    print(f"[info tupay] Descargando transacciones en formato timestamp: {from_timestamp} al {to_timestamp}")
    
    params = {
        "page": 1,
        "from": from_timestamp,
        "to": to_timestamp,
        "country": "PE",
        "refundAttempted": "ALL",
        "sendTo": "developcirot@gmail.com",
        "timezone": "UTC-5"
    }

    current_bearer = bearer_cookie
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            print(f"[info tupay] Solicitando reporte (intento {attempt + 1}/{max_retries})")

            response = requests.get(url, headers=headers, params=params, timeout=500, verify=False)
            
            if response.status_code == 200:
                print("[info tupay] Exito en el envio de reporte")
                return response.status_code, current_bearer
                
            elif response.status_code == 403:
                print("[error tupay] Error 403: Acceso denegado")
                print(f"[debug] Response Content: {response.text[:500]}")
                break
                
            elif response.status_code in [401, 403]:
                print(f"[error tupay] Error de autorizacion {response.status_code}")
                if attempt < max_retries - 1:
                    new_bearer = await session_cache_tupay.get_session(force_refresh=True)
                    if new_bearer:
                        current_bearer = new_bearer
                        headers["Cookie"] = new_bearer
                        print("[info tupay] Sesion Tupay renovada correctamente")
                    else:
                        print("[error tupay] No se pudo renovar sesion Tupay")
                break
                
            else:
                print(f"[error tupay] Status {response.status_code}: {response.text[:200]}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    break
                
        except Exception as e:
            print(f"[error tupay] Excepcion durante solicitud: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
            else:
                break
    
    return None, current_bearer


async def wait_for_tupay_link_async(timeout=1200, interval=60):
    start = time.time()
    print(f"[info tupay] iniciando espera del link de descarga (timeout: {timeout/60:.2f} min)")
    
    while time.time() - start < timeout:
        elapsed_min = (time.time() - start) / 60
        print(f"[info tupay] buscando link en correos... (tiempo transcurrido: {elapsed_min:.2f} min)")
        
        link = get_unread_tupay_link()
        if link:
            print(f"[ok tupay] Link encontrado: {link}")
            print("[info tupay] eliminando todos los correos de Tupay procesados")
            delete_all_tupay_emails()
            return link
            
        print(f"[info tupay] link no disponible aun, proxima revision en {interval} segundos...")
        await asyncio.sleep(interval)  
        
    print(f"[error tupay] No se encontro el link despues de {(time.time() - start)/60:.2f} minutos")
    return None


def delete_all_tupay_emails():
    try:
        mail = imaplib.IMAP4_SSL(Config.IMAP_SERVER)
        mail.login(Config.EMAIL_USER, Config.EMAIL_PASS)
        mail.select("inbox")
        
        # buscar todos los correos de Tupay (leidos y no leidos)
        status, messages = mail.search(None, '(FROM "merchants@tupaypagos.com")')
        email_ids = messages[0].split()
        
        if not email_ids:
            print("[info tupay] No hay correos de Tupay para eliminar")
            mail.logout()
            return True
        
        print(f"[info tupay] Moviendo {len(email_ids)} correo(s) de Tupay a la Papelera")
        
        trash_folder = "[Gmail]/Trash" 
        try:
            mail.select(trash_folder)
            mail.select("inbox")
        except:
            trash_folder = "[Gmail]/Papelera"

        # mover correos a la papelera (Copy + Delete del Inbox)
        for email_id in email_ids:
            mail.copy(email_id, trash_folder)
            mail.store(email_id, '+FLAGS', '\\Deleted')
        
        # eliminar permanentemente los correos marcados del INBOX (ya estan copiados en Trash)
        mail.expunge()
        mail.logout()
        
        print(f"[ok tupay] {len(email_ids)} correo(s) movido(s) a {trash_folder} exitosamente")
        return True
        
    except Exception as e:
        print(f"[error tupay] Error moviendo correos a papelera: {e}")
        return False


def get_unread_tupay_link():
    
    LINK_PATTERN = r"(https://merchants-attachments-prod\.s3\.amazon[^\s\"']+)"
    LINK_PATTERN2 = r"(https://noti[^\s\"']+)"
    
    try:
        mail = imaplib.IMAP4_SSL(Config.IMAP_SERVER)
        mail.login(Config.EMAIL_USER, Config.EMAIL_PASS)
        mail.select("inbox")
        status, messages = mail.search(None, '(UNSEEN FROM "merchants@tupaypagos.com")')
        email_ids = messages[0].split()
        
        if not email_ids:
            mail.logout()
            return None
            
        latest_email_id = email_ids[-1]
        status, data = mail.fetch(latest_email_id, "(RFC822)")
        raw_email = data[0][1]
        msg = email.message_from_bytes(raw_email)

        email_body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() in ["text/html", "text/plain"]:
                    payload = part.get_payload(decode=True)
                    if payload:
                        email_body += payload.decode(errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                email_body = payload.decode(errors="ignore")

        link = None
        match = re.search(LINK_PATTERN, email_body)
        if match:
            link = html.unescape(match.group(1))
        else:
            print("[info tupay] No se encontro el primer link, buscando con segunda opcion")
            match2 = re.search(LINK_PATTERN2, email_body)
            if match2:
                link = html.unescape(match2.group(1))
        
        mail.logout()
        return link
        
    except Exception as e:
        print(f"[error tupay] Error leyendo correo: {e}")
        return None


def download_with_curl(link: str, output_path: str, timeout=300):
    try:
        print(f"[info tupay] Intentando descarga con curl: {link}")
        
        # Comando curl con opciones robustas
        cmd = [
            'curl',
            '-L',  # Seguir redirects
            '-k',  # Ignorar errores SSL
            '--connect-timeout', '30',  # Timeout de conexion
            '--max-time', str(timeout),  # Timeout total
            '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',  # User-Agent
            '-o', output_path,  # Output file
            link
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
        
        if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"[ok tupay] Descarga exitosa con curl, tamaño: {os.path.getsize(output_path)} bytes")
            return True
        else:
            print(f"[error tupay] Curl fallo con codigo: {result.returncode}")
            if result.stderr:
                print(f"[error tupay] Stderr: {result.stderr[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"[error tupay] Curl timeout despues de {timeout}s")
        return False
    except FileNotFoundError:
        print("[warn tupay] curl no esta disponible en el sistema")
        return False
    except Exception as e:
        print(f"[error tupay] Error ejecutando curl: {e}")
        return False


def download_and_upload(link: str, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"[info tupay] Intento de descarga {attempt + 1}/{max_retries} desde: {link}")
            
            session = requests.Session()
            session.verify = False
            response = session.get(
                link, 
                stream=True, 
                timeout=(30, 300),  
                allow_redirects=True, 
                headers=headers
            )
            response.raise_for_status()
            
            print(f"[info tupay] Descarga exitosa, tamaño: {response.headers.get('content-length', 'desconocido')} bytes")
            
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            s3_key = f"digital/collectors/tupay/input/reporte_tupay_{current_time}.csv"
            
            upload_file_to_s3(response.content, s3_key)
            print(f"[ok tupay] Archivo guardado en S3: {s3_key}")
            
            session.close()
            return s3_key
            
        except requests.exceptions.ConnectTimeout as e:
            print(f"[error tupay] Timeout de conexion en intento {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"[info tupay] Esperando {wait_time}s antes de reintentar...")
                time.sleep(wait_time)
            else:
                print(f"[warn tupay] Fallo despues de {max_retries} intentos con requests - intentando curl...")
                
        except requests.exceptions.ReadTimeout as e:
            print(f"[error tupay] Timeout de lectura en intento {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"[info tupay] Esperando {wait_time}s antes de reintentar...")
                time.sleep(wait_time)
            else:
                print(f"[warn tupay] Fallo despues de {max_retries} intentos con requests - intentando curl...")
                
        except requests.exceptions.RequestException as e:
            print(f"[error tupay] Error de red en intento {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"[info tupay] Esperando {wait_time}s antes de reintentar...")
                time.sleep(wait_time)
            else:
                print(f"[warn tupay] Fallo despues de {max_retries} intentos con requests - intentando curl...")
                
        except Exception as e:
            print(f"[error tupay] Error descargando archivo: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                print(f"[info tupay] Esperando {wait_time}s antes de reintentar...")
                time.sleep(wait_time)
        finally:
            try:
                session.close()
            except:
                pass
    
    # Si requests fallo, intentar con curl como fallback
    print("[info tupay] Requests fallo, intentando metodo alternativo con curl...")
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        if download_with_curl(link, tmp_path, timeout=300):
            # Leer archivo descargado y subir a S3
            with open(tmp_path, 'rb') as f:
                file_content = f.read()
            
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            s3_key = f"digital/collectors/tupay/input/reporte_tupay_{current_time}.csv"
            
            upload_file_to_s3(file_content, s3_key)
            print(f"[ok tupay] Archivo guardado en S3 usando curl: {s3_key}")
            
            # Limpiar archivo temporal
            try:
                os.unlink(tmp_path)
            except:
                pass
            
            return s3_key
        else:
            print("[error tupay] Curl tambien fallo")
            # Limpiar archivo temporal
            try:
                os.unlink(tmp_path)
            except:
                pass
    except Exception as e:
        print(f"[error tupay] Error en fallback curl: {e}")
    
    return None


# =============================
#   FUNCION PRINCIPAL
# =============================
async def get_data_main_async(from_date, to_date):
    try:
        print(f"[info tupay] Procesando Tupay desde {from_date} hasta {to_date}")
        
        # Obtener sesion del cache
        bearer_cookie = await session_cache_tupay.get_session()
        if not bearer_cookie:
            print("[error tupay] No se pudo obtener sesion de Tupay")
            return False

        # Solicitar reporte
        status, final_bearer = await get_data_json_tupay_async(bearer_cookie, from_date, to_date)
        
        if status == 200:
            print("[info tupay] Reporte solicitado exitosamente, esperando link por correo")
            
            # Esperar link por correo
            link = await wait_for_tupay_link_async(timeout=1800, interval=60)
            if not link:
                print("[error tupay] No se encontro link en correos")
                return False
                
            print("[info tupay] Link encontrado con exito, descargando archivo")
            
            # Descargar y subir archivo
            s3_key = download_and_upload(link)
            if s3_key:
                print("[ok tupay] Proceso Tupay completado exitosamente")
                return True
            else:
                print("[error tupay] No se pudo descargar el archivo")
                return False
        else:
            print("[error tupay] No se pudo solicitar el reporte")
            return False
        
    except Exception as e:
        print(f"[error tupay] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    start_time = time.time()
    print(f"\n{'='*50}")
    print(f"[inicio tupay] proceso extraccion async tupay | rango: {from_date.date()} a {to_date.date()}")
    print(f"{'='*50}\n")
    print(f"[wrapper] ejecutando tupay collector")
    
    success = False
    try:
        success = asyncio.run(get_data_main_async(from_date, to_date))
    except Exception as e:
        print(f"[error tupay] error en get_data_main: {e}")
        
    elapsed_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"[fin] proceso tupay completado")
    print(f"[tiempo] duracion total: {elapsed_time / 60:.2f} minutos")
    print(f"{'='*50}\n")
    return success



