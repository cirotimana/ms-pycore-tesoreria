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


# =============================
#   CACHE DE TOKEN KUSHKI
# =============================
class TokenCacheKushki:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.token and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token Kushki del cache")
                return self.token

            print("[INFO] Obteniendo nuevo token Kushki...")
            self.token = await get_token_kushki()
            
            if self.token:
                # Token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token Kushki cacheado hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nuevo token Kushki")
            
            return self.token

    def invalidate(self):
        print("[INFO] Invalidando token Kushki cacheado")
        self.token = None
        self.expires_at = None


token_cache_kushki = TokenCacheKushki()


# =============================
#   OBTENER TOKEN
# =============================
async def get_token_kushki():
    
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Kushki")
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
            token_found = None

            async def handle_request(route, request):
                nonlocal token_found
                headers = request.headers
                if 'authorization' in headers and not token_found:
                    token_found = headers['authorization']
                    print(f"[DEBUG] Token capturado: {token_found[:20]}...")
                await route.continue_()

            await context.route("**", handle_request)

            print("[INFO] Navegando a Kushki")
            await page.goto("https://console.kushkipagos.com/auth", wait_until="networkidle", timeout=60000)

            # Login
            print("[INFO] Realizando login")
            await page.fill('input[name="username"], input[type="email"], input[type="text"]', Config.USER_NAME_KUSHKI)
            await page.fill('input[name="password"]', Config.PASSWORD_KUSHKI)
            await page.click('button[type="submit"], #kc-login')

            # Esperar captura del token
            print("[INFO] Esperando captura del token (30s max)")
            for i in range(30):
                if token_found:
                    print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                    break
                await asyncio.sleep(1)

            if token_found:
                print("[INFO] Token capturado exitosamente")
                return token_found
            else:
                print("[ERROR] No se encontro token despues de 30 segundos")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_kushki: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_kushki(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass


async def close_playwright_resources_kushki(browser, context, page):
    print("[INFO] Cerrando recursos de Kushki...")
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
            print("[DEBUG] Recursos de Kushki cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de Kushki: {e}")


# =============================
#   FUNCIONES DE UTILIDAD 
# =============================
def convert_utc_to_local(utc_str):
    if not utc_str:
        return None
    
    lima_tz = pytz.timezone("America/Lima")
    
    try:
        dt_utc = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            dt_utc = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            print(f"[INFO] Formato de fecha no reconocido: {utc_str} - Error: {str(e)}")
            return None
    
    dt_utc = dt_utc.replace(tzinfo=pytz.UTC)
    dt_local = dt_utc.astimezone(lima_tz)
    
    return dt_local.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def extract_external_id(metadata):
    
    if not metadata:
        return ""
    
    try:
        if isinstance(metadata, str):
            import ast
            metadata = ast.literal_eval(metadata)
        
        if 'externalID' in metadata:
            return str(metadata['externalID'])
        
        if 'operationId' in metadata:
            operation_id = str(metadata['operationId'])
            
            if operation_id.endswith('-ATP'):
                operation_id = operation_id[:-4]
            operation_id = operation_id.replace('-', '.')
            
            return operation_id
        
        return ""
    
    except:
        return ""


# =============================
#   FUNCIONES DE DATOS 
# =============================
async def get_data_json_kushki_async(token, from_date, to_date):
    
    start_date = from_date.replace(hour=0, minute=0, second=0) 
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    
    print(f"[INFO] Descargando transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }

    url = "https://api.kushkipagos.com/analytics/v1/admin/merchant/search"
    limit = 250

    total_records = 0
    current_token = token

    current = start_date
    while current < end_date:
        from_dt = current.replace(hour=0, minute=0, second=0)
        to_dt = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0)

        from_date_str = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        to_date_str = to_dt.strftime("%Y-%m-%dT%H:%M:%S")

        print(f"[INFO] Descargando transacciones del {current.strftime('%Y-%m-%d')}")

        all_data = []
        offset = 0
        has_more_data = True
        
        while has_more_data:
            body = {
                "filter": {},
                "from": from_date_str,
                "limit": limit,
                "offset": offset,
                "rangeAmount": {},
                "sort": {
                    "field": "created",
                    "order": "desc"
                },
                "text": "",
                "timeZone": "-05:00",
                "to": to_date_str
            }

            max_retries = 5
            success = False
            registros = []
            
            for retry in range(max_retries):
                try:
                    response = requests.post(url, headers=headers, json=body, timeout=120)
                    
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
                            print(f"[INFO] No hay mas registros para {current.strftime('%Y-%m-%d')}")
                            has_more_data = False
                            success = True
                            break
                            
                    elif response.status_code in [401, 403]:
                        print(f"[ERROR] Error de autorizacion {response.status_code}")
                        if retry < max_retries - 1:
                            new_token = await token_cache_kushki.get_token(force_refresh=True)
                            if new_token:
                                current_token = new_token
                                headers["Authorization"] = current_token
                                print("[INFO] Token renovado para descarga")
                            else:
                                print("[ERROR] No se pudo renovar token")
                        break
                        
                    elif response.status_code == 504:
                        print(f"[WARN] Error 504 (timeout) en offset {offset}, reintento {retry + 1}")
                        if retry < max_retries - 1:
                            await asyncio.sleep(5)
                        else:
                            has_more_data = False
                            break
                            
                    elif response.status_code == 400:
                        print(f"[WARN] Error 400 en offset {offset}, reintento {retry + 1}")
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
        
        print(f"[INFO] Descarga completa para {current.strftime('%Y-%m-%d')}, total registros: {len(all_data)}")
        total_records += len(all_data)
        
        # Guardar datos del dia
        if all_data:
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            file_key = f"digital/collectors/kushki/input/response_{current_time}.json"
            upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
            print(f"[SUCCESS] Archivo guardado en S3: {file_key}")
            
        current += timedelta(days=1)
        
        # Pausa entre dias
        if current < end_date:
            await asyncio.sleep(2)
            
    print(f"[INFO] Total general de registros descargados: {total_records}")
    return total_records, current_token


def json_excel_kushki():
    prefix = "digital/collectors/kushki/input/"
    files = list_files_in_s3(prefix)
    
    processed_count = 0

    for file_key in files:
        if not file_key.endswith(".json") or "/input/processed/" in file_key:
            continue

        print(f"[INFO] Procesando {file_key}")
        try:
            content = read_file_from_s3(file_key)
            data = json.loads(content.decode("utf-8"))
            
            if isinstance(data, dict):
                data = data.get("data") or data.get("results") or []
            elif isinstance(data, list):
                pass  # data ya es la lista de registros
            else:
                data = []

            if not data or not isinstance(data, list):
                print("[INFO] No hay datos validos para procesar")
                continue

            rows = []
            for item in data:
                source = item.get("_source", {})
                metadata = source.get("metadata", {})
                
                row = {
                    "ticket_number": source.get("ticket_number", ""),
                    "external_id": extract_external_id(metadata),
                    "approval_code": source.get("approval_code", ""),
                    "payment_method": source.get("payment_method", ""),
                    "payment_submethod_type": source.get("payment_submethod_type", ""),
                    "channel": source.get("channel", ""),
                    "created": convert_utc_to_local(source.get("created")) if source.get("created") else None,
                    "merchant_id": source.get("merchant_id", ""),
                    "merchant_name": source.get("merchant_name", ""),
                    "response_code": source.get("response_code", ""),
                    "response_text": source.get("response_text", ""),
                    "transaction_status": source.get("transaction_status", ""),
                    "card_country": source.get("card_country", ""),
                    "card_holder_name": source.get("card_holder_name", ""),
                    "payment_brand": source.get("payment_brand", ""),
                    "transaction_type": source.get("transaction_type", ""),
                    "currency_code": source.get("currency_code", ""),
                    "sale_ticket_number": source.get("sale_ticket_number", ""),
                    "masked_credit_card": source.get("masked_credit_card", ""),
                    "approved_transaction_amount": source.get("approved_transaction_amount", ""),
                    "subtotal_iva": source.get("subtotal_iva", ""),
                    "subtotal_iva0": source.get("subtotal_iva0", ""),
                    "ice_value": source.get("ice_value", ""),
                    "iva_value": source.get("iva_value", ""),
                    "taxes": source.get("taxes", ""),
                    "number_of_months": source.get("number_of_months", ""),
                    "metadata": source.get("metadata", ""),
                    "subscription_id": source.get("subscription_id", ""),
                    "subscription_metadata": source.get("subscription_metadata", ""),
                    "bank_name": source.get("bank_name", ""),
                    "document_number": source.get("document_number", ""),
                    "retry": source.get("retry", ""),
                    "retry_count": source.get("retry_count", ""),
                    "processor_name": source.get("processor_name", ""),
                    "recap": source.get("recap", ""),
                    "pin": source.get("pin", ""),
                    "client_name": source.get("client_name", ""),
                    "identification": source.get("identification", ""),
                    "issuing_bank": source.get("issuing_bank", ""),
                    "security_code": source.get("security_code", ""),
                    "secure_message": source.get("secure_message", ""),
                    "card_type": source.get("card_type", ""),
                    "acquirer_bank": source.get("acquirer_bank", ""),
                    "request_amount": source.get("request_amount", ""),
                    "processor_code": source.get("processor_code", ""),
                    "processor_message": source.get("processor_message", ""),
                    "processor_payment_point": source.get("processor_payment_point", ""),
                    "transaction_card_id": source.get("transaction_card_id", ""),
                    "transaction_cycle": source.get("transaction_cycle", ""),
                    "email": source.get("contact_details", {}).get("email"),
                    "phone_number": source.get("phone_number", ""),
                    "completed": source.get("completed", "")
                }
                rows.append(row)

            df = pd.DataFrame(rows)

            # Guardar Excel en memoria
            with BytesIO() as excel_buffer:
                df.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                output_key = file_key.replace(".json", ".xlsx")
                upload_file_to_s3(excel_buffer.getvalue(), output_key)
            
            # Mover el JSON a processed
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
        print(f"[INICIO] Procesando Kushki desde {from_date} hasta {to_date}")
        
        # Obtener token del cache
        token = await token_cache_kushki.get_token()
        if not token:
            print("[ERROR] No se pudo obtener token de Kushki")
            return False

        # Descargar datos
        data_count, final_token = await get_data_json_kushki_async(token, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[INFO] {data_count} registros descargados, generando archivo Excel")
            json_excel_kushki()
            print("[SUCCESS] Proceso Kushki completado exitosamente")
            return True
        else:
            print("[INFO] No hay datos para procesar")
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    print(f"[WRAPPER] Ejecutando Kushki collector")
    return asyncio.run(get_data_main_async(from_date, to_date))


