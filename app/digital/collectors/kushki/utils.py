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
                print("[info] usando token kushki del cache")
                return self.token

            print("[info] obteniendo nuevo token kushki...")
            self.token = await get_token_kushki()
            
            if self.token:
                # token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[info] token kushki cacheado hasta {self.expires_at}")
            else:
                print("[error] no se pudo obtener nuevo token kushki")
            
            return self.token

    def invalidate(self):
        print("[info] invalidando token kushki cacheado")
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
            print("[info] lanzando navegador para kushki")
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
                    print(f"[debug] token capturado: {token_found[:20]}...")
                # ignorar error cuando el contexto ya fue cerrado
                try:
                    await route.continue_()
                except Exception:
                    pass

            await context.route("**", handle_request)

            print("[info] navegando a kushki")
            await page.goto("https://console.kushkipagos.com/auth", wait_until="networkidle", timeout=60000)

            # login
            print("[info] realizando login")
            await page.fill('input[name="username"], input[type="email"], input[type="text"]', Config.USER_NAME_KUSHKI)
            await page.fill('input[name="password"]', Config.PASSWORD_KUSHKI)
            await page.click('button[type="submit"], #kc-login')

            # esperar captura del token
            print("[info] esperando captura del token (30s max)")
            for i in range(30):
                if token_found:
                    print(f"[ok] token capturado en segundo {i + 1}")
                    break
                await asyncio.sleep(1)

            if token_found:
                print("[info] token capturado exitosamente")
                # limpiar rutas pendientes antes de cerrar el contexto
                try:
                    await page.unroute_all(behavior='ignoreErrors')
                except Exception:
                    pass
                return token_found
            else:
                print("[error] no se encontro token despues de 30 segundos")
                try:
                    await page.unroute_all(behavior='ignoreErrors')
                except Exception:
                    pass
                return None
                
    except Exception as e:
        print(f"[error] error general en get_token_kushki: {e}")
        return None
        
    finally:
        # cierre garantizado de recursos
        await close_playwright_resources_kushki(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[debug] playwright cerrado completamente.")
        except Exception:
            pass


async def close_playwright_resources_kushki(browser, context, page):
    print("[info] cerrando recursos de kushki...")
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
            print("[debug] recursos de kushki cerrados correctamente")
        except Exception as e:
            print(f"[warn] error cerrando recursos de kushki: {e}")


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
            print(f"[info] formato de fecha no reconocido: {utc_str} - error: {str(e)}")
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
    # descarga registros del rango completo paginando por offset en una sola sesion
    from_dt = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    to_dt = to_date.replace(hour=23, minute=59, second=59, microsecond=0)

    from_date_str = from_dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    to_date_str = to_dt.strftime("%Y-%m-%dT%H:%M:%S.000")

    print(f"[info] descargando rango completo desde {from_date_str} hasta {to_date_str}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }

    url = "https://api.kushkipagos.com/analytics/v1/admin/merchant/search"
    limit = 500
    current_token = token

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

        for retry in range(max_retries):
            try:
                response = await asyncio.to_thread(
                    requests.post, url, headers=headers, json=body, timeout=120
                )

                if response.status_code == 200:
                    data = response.json()
                    registros = data.get('data', data) if isinstance(data, dict) else data

                    if registros:
                        all_data.extend(registros)
                        print(f"[info] descargados {len(registros)} registros (offset {offset})")

                        if len(registros) < limit:
                            has_more_data = False
                        else:
                            offset += limit

                        success = True
                        break
                    else:
                        print("[info] no hay mas registros")
                        has_more_data = False
                        success = True
                        break

                elif response.status_code in [401, 403]:
                    print(f"[error] error de autorizacion {response.status_code}")
                    if retry < max_retries - 1:
                        new_token = await token_cache_kushki.get_token(force_refresh=True)
                        if new_token:
                            current_token = new_token
                            headers["Authorization"] = current_token
                            print("[info] token renovado")
                        else:
                            print("[error] no se pudo renovar token")
                    break

                elif response.status_code in [504, 400]:
                    wait_time = (retry + 1) * 5
                    print(f"[warn] error {response.status_code}, reintento {retry + 1}, esperando {wait_time}s")
                    await asyncio.sleep(wait_time)

                else:
                    print(f"[error] status {response.status_code}: {response.text[:200]}")
                    has_more_data = False
                    break

            except Exception as e:
                print(f"[error] excepcion durante descarga: {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    has_more_data = False
                    break

        if not success:
            print(f"[error] no se pudo descargar datos para offset {offset}")
            break

        # pausa minima entre paginas
        if has_more_data:
            await asyncio.sleep(0.5)

    print(f"[info] total registros descargados: {len(all_data)}")

    if all_data:
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        file_key = f"digital/collectors/kushki/input/response_{current_time}.json"
        upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
        print(f"[ok] archivo guardado en s3: {file_key}")

    return len(all_data), current_token



def json_excel_kushki():
    prefix = "digital/collectors/kushki/input/"
    files = list_files_in_s3(prefix)
    
    processed_count = 0

    for file_key in files:
        if not file_key.endswith(".json") or "/input/processed/" in file_key:
            continue

        print(f"[info] procesando {file_key}")
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
                print("[info] no hay datos validos para procesar")
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
            print(f"[ok] procesado: {file_key} -> {output_key}")

        except Exception as e:
            print(f"[error] error procesando {file_key}: {e}")
            continue

    print(f"[info] proceso json -> excel completado. archivos procesados: {processed_count}")


# =============================
#   FUNCION PRINCIPAL 
# =============================
async def get_data_main_async(from_date, to_date):
    try:
        print(f"[inicio] procesando kushki desde {from_date} hasta {to_date}")
        
        # obtener token del cache
        token = await token_cache_kushki.get_token()
        if not token:
            print("[error] no se pudo obtener token de kushki")
            return False

        # descargar datos
        data_count, final_token = await get_data_json_kushki_async(token, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[info] {data_count} registros descargados, generando archivo excel")
            json_excel_kushki()
            print("[ok] proceso kushki completado exitosamente")
            return True
        else:
            print("[info] no hay datos para procesar")
            return False
        
    except Exception as e:
        print(f"[error] error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    # wrapper sincrono que ejecuta el proceso principal de kushki y mide el tiempo de ejecucion
    start_time = time.time()

    print(f"\n{'='*50}")
    print(f"[inicio] proceso kushki | rango: {from_date.date()} a {to_date.date()}")
    print(f"{'='*50}\n")

    try:
        result = asyncio.run(get_data_main_async(from_date, to_date))
    except Exception as e:
        print(f"[error] fallo ejecucion principal kushki: {e}")
        result = False
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"\n{'='*50}")
        print(f"[fin] proceso kushki completado")
        print(f"[tiempo] duracion total: {elapsed_time / 60:.2f} minutos")
        print(f"{'='*50}\n")

    return result
