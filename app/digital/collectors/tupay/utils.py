import asyncio
from playwright.async_api import async_playwright
import requests
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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# =============================
#   CACHE DE SESION TUPAY
# =============================
class SessionCacheTupay:
    def __init__(self):
        self.bearer_cookie = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_session(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.bearer_cookie and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando sesion Tupay del cache")
                return self.bearer_cookie

            print("[INFO] Obteniendo nueva sesion Tupay...")
            self.bearer_cookie = await get_token_tupay()
            
            if self.bearer_cookie:
                # Sesion valida por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Sesion Tupay cacheada hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nueva sesion Tupay")
            
            return self.bearer_cookie

    def invalidate(self):
        print("[INFO] Invalidando sesion Tupay cacheada")
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
            print("[INFO] Lanzando navegador para Tupay")
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

            try:
                print("[INFO] Navegando a Tupay")
                await page.goto("https://merchants.tupaypagos.com/login", wait_until="networkidle", timeout=90000)
                
                # Login
                print("[INFO] Realizando login")
                await page.fill('input[name="email"], input[type="email"], input[type="text"]', Config.USER_NAME_TUPAY)
                await page.fill('input[name="password"], input[type="password"]', Config.PASSWORD_TUPAY)
                await page.click('button[type="submit"]')
                
                # Esperar redireccion
                print("[INFO] Esperando completar login")
                await page.wait_for_url("**/home**", timeout=30000)

                # Capturar cookies
                cookies = await context.cookies()
                cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                
                if 'BEARER_TOKEN=' in cookie_header:
                    for part in cookie_header.split(';'):
                        if part.strip().startswith('BEARER_TOKEN='):
                            bearer_cookie = part.replace(' ', '').strip()

                if bearer_cookie:
                    print("[INFO] Bearer Cookie capturada exitosamente")
                    return bearer_cookie
                else:
                    print("[ERROR] No se encontraron bearer cookies")
                    return None

            except Exception as e:
                print(f"[ERROR] Error durante la captura: {e}")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_tupay: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_tupay(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass



async def close_playwright_resources_tupay(browser, context, page):
    print("[INFO] Cerrando recursos de Tupay...")
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
            print("[DEBUG] Recursos de Tupay cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de Tupay: {e}")


# =============================
#   FUNCIONES DE DATOS
# =============================
async def get_data_json_tupay_async(bearer_cookie, from_date, to_date):
    
    start_date = from_date.replace(hour=0, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    
    from_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S") 
    to_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[INFO] Descargando transacciones del: {from_date_str} al {to_date_str}")
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": bearer_cookie
    }
    
    url = "https://merchants-api.tupayonline.com/v1/deposits/export"
    
    from_timestamp = date_to_timestamp(from_date_str)
    to_timestamp = date_to_timestamp(to_date_str)

    print(f"[INFO] Descargando transacciones en formato timestamp: {from_timestamp} al {to_timestamp}")
    
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
            print(f"[INFO] Solicitando reporte (intento {attempt + 1}/{max_retries})")

            response = requests.get(url, headers=headers, params=params, timeout=500, verify=False)
            
            if response.status_code == 200:
                print("[INFO] Exito en el envio de reporte")
                return response.status_code, current_bearer
                
            elif response.status_code == 403:
                print("[ERROR] Error 403: Acceso denegado")
                print(f"[DEBUG] Response Content: {response.text[:500]}")
                break
                
            elif response.status_code in [401, 403]:
                print(f"[ERROR] Error de autorizacion {response.status_code}")
                if attempt < max_retries - 1:
                    new_bearer = await session_cache_tupay.get_session(force_refresh=True)
                    if new_bearer:
                        current_bearer = new_bearer
                        headers["Cookie"] = new_bearer
                        print("[INFO] Sesion Tupay renovada correctamente")
                    else:
                        print("[ERROR] No se pudo renovar sesion Tupay")
                break
                
            else:
                print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    break
                
        except Exception as e:
            print(f"[ERROR] Excepcion durante solicitud: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
            else:
                break
    
    return None, current_bearer


async def wait_for_tupay_link_async(timeout=1200, interval=60):
    start = time.time()
    while time.time() - start < timeout:
        link = get_unread_tupay_link()
        print(f"[INFO] Link encontrado: {link}")
        if link:
            print("[INFO] Link encontrado en correo, eliminando todos los correos de Tupay")
            delete_all_tupay_emails()
            return link
        print(f"[INFO] Link no disponible aun, esperando {interval} segundos...")
        await asyncio.sleep(interval)  
        
    print("[ERROR] No se encontro el link en el tiempo maximo de espera")
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
            print("[INFO] No hay correos de Tupay para eliminar")
            mail.logout()
            return True
        
        print(f"[INFO] Moviendo {len(email_ids)} correo(s) de Tupay a la Papelera")
        
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
        
        print(f"[SUCCESS] {len(email_ids)} correo(s) movido(s) a {trash_folder} exitosamente")
        return True
        
    except Exception as e:
        print(f"[ERROR] Error moviendo correos a papelera: {e}")
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
            print("[INFO] No se encontro el primer link, buscando con segunda opcion")
            match2 = re.search(LINK_PATTERN2, email_body)
            if match2:
                link = html.unescape(match2.group(1))
        
        mail.logout()
        return link
        
    except Exception as e:
        print(f"[ERROR] Error leyendo correo: {e}")
        return None


def download_and_upload(link: str):
    try:
        response = requests.get(link, stream=True)
        response.raise_for_status()

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        s3_key = f"digital/collectors/tupay/input/reporte_tupay_{current_time}.csv"

        upload_file_to_s3(response.content, s3_key)
        print(f"[SUCCESS] Archivo guardado en S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"[ERROR] Error descargando archivo: {e}")
        return None


# =============================
#   FUNCION PRINCIPAL
# =============================
async def get_data_main_async(from_date, to_date):
    try:
        print(f"[INICIO] Procesando Tupay desde {from_date} hasta {to_date}")
        
        # Obtener sesion del cache
        bearer_cookie = await session_cache_tupay.get_session()
        if not bearer_cookie:
            print("[ERROR] No se pudo obtener sesion de Tupay")
            return False

        # Solicitar reporte
        status, final_bearer = await get_data_json_tupay_async(bearer_cookie, from_date, to_date)
        
        if status == 200:
            print("[INFO] Reporte solicitado exitosamente, esperando link por correo")
            
            # Esperar link por correo
            link = await wait_for_tupay_link_async(timeout=1800, interval=60)
            if not link:
                print("[ERROR] No se encontro link en correos")
                return False
                
            print("[INFO] Link encontrado con exito, descargando archivo")
            
            # Descargar y subir archivo
            s3_key = download_and_upload(link)
            if s3_key:
                print("[SUCCESS] Proceso Tupay completado exitosamente")
                return True
            else:
                print("[ERROR] No se pudo descargar el archivo")
                return False
        else:
            print("[ERROR] No se pudo solicitar el reporte")
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    print(f"[WRAPPER] Ejecutando Tupay collector")
    return asyncio.run(get_data_main_async(from_date, to_date))


