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
from app.digital.collectors.monnet.get_qr_2mf.use_secret import main as get_validator_main


# =============================
#   CACHE DE TOKEN MONNET
# =============================
class TokenCacheMonnet:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.token and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token Monnet del cache")
                return self.token

            print("[INFO] Obteniendo nuevo token Monnet...")
            self.token = await get_token_monnet()
            
            if self.token:
                # Token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token Monnet cacheado hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nuevo token Monnet")
            
            return self.token

    def invalidate(self):
        print("[INFO] Invalidando token Monnet cacheado")
        self.token = None
        self.expires_at = None


token_cache_monnet = TokenCacheMonnet()


# =============================
#   OBTENER TOKEN
# =============================
async def get_token_monnet():
    
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para Monnet")
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

            print("[INFO] Navegando a Monnet")
            await page.goto("https://payin.monnetpayments.com/pages/auth/login", wait_until="networkidle", timeout=60000)

            # Login
            print("[INFO] Realizando login")
            await page.fill('input[name="username"], input[type="email"]', Config.USER_NAME_MONNET)
            await page.fill('input[name="password"]', Config.PASSWORD_MONNET)
            await page.click('button[type="submit"], #kc-login')

            # Esperar a que se cargue el contenido
            print("[INFO] Esperando carga post-login")
            await page.wait_for_selector("label", timeout=10000)

            # Buscar el label que contiene el texto del usuario
            labels = await page.query_selector_all("label")
            checkbox_found = False
            user = Config.USER_NAME_MONNET

            for label in labels:
                label_text = (await label.inner_text()).strip().lower()
                if user in label_text:
                    print(f"[INFO] Label encontrado para '{user}'")
                    await label.click(force=True)
                    checkbox_found = True
                    print("[INFO] Label clickeado exitosamente")
                    break

            if not checkbox_found:
                print(f"[WARN] No se encontro el label para '{user}'")

            # Esperar inputs OTP
            print("[INFO] Esperando campo OTP")
            await page.wait_for_selector('#otp-group input[type="text"]', timeout=10000)
            
            validator = get_validator_main()
            validator_str = str(validator).strip()
            
            if len(validator_str) == 6 and validator_str.isdigit():
                print(f"[INFO] Ingresando codigo OTP: {validator_str}")
                for i, digit in enumerate(validator_str, start=1):
                    await page.fill(f'#otp{i}', digit)
                    await asyncio.sleep(0.5)
                await asyncio.sleep(1)
                await page.keyboard.press("Enter")
                print("[INFO] Codigo OTP ingresado y Enter enviado")
            else:
                print(f"[ERROR] Codigo OTP invalido: {validator_str}")
                return None

            # Esperar captura del token
            print("[INFO] Esperando captura del token (60s max)")
            for i in range(60):
                if token_found:
                    print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                    break
                await asyncio.sleep(1)

            if token_found:
                print("[INFO] Token capturado exitosamente")
                return token_found
            else:
                print("[ERROR] No se encontro token despues de 60 segundos")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_monnet: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_monnet(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass


async def close_playwright_resources_monnet(browser, context, page):
    print("[INFO] Cerrando recursos de Monnet...")
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
            print("[DEBUG] Recursos de Monnet cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de Monnet: {e}")


# =============================
#   FUNCIONES DE DATOS
# =============================
# async def get_data_json_monnet_async(token, from_date, to_date):
#     print(f"[INFO] Descargando transacciones del: {from_date.strftime('%Y-%m-%d')} al {to_date.strftime('%Y-%m-%d')}")

#     headers = {
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#         "Authorization": token
#     }

#     url = "https://apiin.monnetpayments.com/ms-experience-front/operation-return/api/report/transaction-report?isFilPaymentDate=0"

#     from_d = from_date.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
#     to_d = to_date.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")

#     body = {
#         "filters": {
#             "startDate": from_d,
#             "endDate": to_d,
#             "merchantIds": [226],
#             "page": 0,
#             "size": 1000000,
#             "chargeBack": None
#         }
#     }

#     print(f"[INFO] Descargando transacciones del {from_d} al {to_d}")

#     max_retries = 3
#     current_token = token
    
#     for retry in range(max_retries):
#         try:
#             response = requests.post(url, headers=headers, json=body, timeout=180)  # Timeout aumentado
            
#             if response.status_code == 200:
#                 current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
#                 file_key = f"digital/collectors/monnet/input/response_{current_time}.json"
                
#                 # Validar que la respuesta contenga datos
#                 try:
#                     response_data = response.json()
#                     if isinstance(response_data, dict) and response_data.get("data"):
#                         data_count = len(response_data["data"])
#                         print(f"[INFO] Datos recibidos: {data_count} registros")
#                     else:
#                         print("[WARN] Respuesta no contiene datos esperados")
#                 except:
#                     print("[INFO] No se pudo parsear JSON de respuesta, guardando contenido crudo")
                
#                 upload_file_to_s3(response.content, file_key)
#                 print(f"[SUCCESS] Archivo guardado en S3: {file_key}")
#                 return len(response.content), current_token
                
#             elif response.status_code in [401, 403]:
#                 print(f"[ERROR] Error de autorizacion {response.status_code}")
#                 if retry < max_retries - 1:
#                     new_token = await token_cache_monnet.get_token(force_refresh=True)
#                     if new_token:
#                         current_token = new_token
#                         headers["Authorization"] = current_token
#                         print("[INFO] Token renovado para descarga")
#                     else:
#                         print("[ERROR] No se pudo renovar token")
#                 break
                
#             else:
#                 print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
#                 if retry < max_retries - 1:
#                     print(f"[INFO] Reintentando en 5 segundos...")
#                     await asyncio.sleep(5)
#                 else:
#                     break
                    
#         except requests.exceptions.Timeout:
#             print(f"[ERROR] Timeout en la solicitud, reintento {retry + 1}")
#             if retry < max_retries - 1:
#                 await asyncio.sleep(5)
#             else:
#                 break
                
#         except Exception as e:
#             print(f"[ERROR] Excepcion durante descarga: {e}")
#             if retry < max_retries - 1:
#                 await asyncio.sleep(5)
#             else:
#                 break
    
#     return None, current_token

async def get_data_json_monnet_async(token, from_date, to_date):
    print(f"[INFO] Descargando transacciones del: {from_date.strftime('%Y-%m-%d')} al {to_date.strftime('%Y-%m-%d')}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }

    url = "https://apiin.monnetpayments.com/ms-experience-front/operation-return/api/report/transaction-report?isFilPaymentDate=0"

    from_d = from_date.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
    to_d = to_date.replace(hour=0, minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")

    body = {
        "filters": {
            "startDate": from_d,
            "endDate": to_d,
            "merchantIds": [226],
            "page": 0,
            "size": 1000000,
            "chargeBack": None
        }
    }

    print(f"[INFO] Descargando transacciones del {from_d} al {to_d}")

    max_retries = 3
    current_token = token
    
    for retry in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=body, timeout=180)
            
            if response.status_code == 200:
                # Validar SI la respuesta contiene datos reales
                try:
                    response_data = response.json()
                    
                    # Detectar si es un error de autenticacion
                    if isinstance(response_data, dict):
                        if response_data.get("codigo") == "41":  # Error de autenticacion
                            print(f"[ERROR] Autenticacion fallida: {response_data.get('mensaje')}")
                            if retry < max_retries - 1:
                                print("[INFO] Token invalido, renovando...")
                                new_token = await token_cache_monnet.get_token(force_refresh=True)
                                if new_token:
                                    current_token = new_token
                                    headers["Authorization"] = new_token
                                    print("[INFO] Token renovado, reintentando...")
                                    continue  # Reintentar con nuevo token
                                else:
                                    print("[ERROR] No se pudo renovar el token")
                                    break
                            else:
                                print("[ERROR] Maximos reintentos de autenticacion alcanzados")
                                break
                        
                        # Si contiene datos reales
                        elif response_data.get("data") is not None:
                            data_count = len(response_data["data"]) if isinstance(response_data["data"], list) else 0
                            print(f"[INFO] Datos recibidos: {data_count} registros")
                            
                            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
                            file_key = f"digital/collectors/monnet/input/response_{current_time}.json"
                            upload_file_to_s3(response.content, file_key)
                            print(f"[SUCCESS] Archivo guardado en S3: {file_key}")
                            return data_count, current_token
                        
                        else:
                            print(f"[WARN] Respuesta no contiene datos: {response_data}")
                            return 0, current_token
                    
                except json.JSONDecodeError:
                    print("[ERROR] No se pudo decodificar la respuesta JSON")
                    # Guardar respuesta cruda para debugging
                    current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
                    debug_key = f"digital/collectors/monnet/debug/response_error_{current_time}.txt"
                    upload_file_to_s3(response.content, debug_key)
                    print(f"[DEBUG] Respuesta cruda guardada: {debug_key}")
                    break
                
            elif response.status_code in [401, 403]:
                print(f"[ERROR] Error de autorizacion {response.status_code}")
                if retry < max_retries - 1:
                    new_token = await token_cache_monnet.get_token(force_refresh=True)
                    if new_token:
                        current_token = new_token
                        headers["Authorization"] = current_token
                        print("[INFO] Token renovado para descarga")
                    else:
                        print("[ERROR] No se pudo renovar token")
                break
                
            else:
                print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
                if retry < max_retries - 1:
                    print(f"[INFO] Reintentando en 5 segundos...")
                    await asyncio.sleep(5)
                else:
                    break
                    
        except requests.exceptions.Timeout:
            print(f"[ERROR] Timeout en la solicitud, reintento {retry + 1}")
            if retry < max_retries - 1:
                await asyncio.sleep(5)
            else:
                break
                
        except Exception as e:
            print(f"[ERROR] Excepcion durante descarga: {e}")
            if retry < max_retries - 1:
                await asyncio.sleep(5)
            else:
                break
    
    return 0, current_token

def json_excel_monnet():
    prefix = "digital/collectors/monnet/input/"
    files = list_files_in_s3(prefix)
    
    processed_count = 0

    for file_key in files:
        if not file_key.endswith(".json") or "/input/processed/" in file_key:
            continue

        print(f"[INFO] Procesando {file_key}")
        try:
            content = read_file_from_s3(file_key)
            data = json.loads(content.decode("utf-8"))

            data = data.get("data") or data.get("results") or []

            if not data or not isinstance(data, list):
                print("[INFO] No hay datos validos para procesar")
                continue

            rows = []
            for item in data:
                row = {
                    "Origen": item.get("origin"),
                    "Id Operacion": item.get("idOperation"),
                    "Id Operacion Comercio": item.get("merchantOperationNumber"),
                    "Codigo de pago": item.get("paymentCode"),
                    "ID procesador": item.get("processorId"),
                    "Canal de Pago": item.get("channel"),
                    "BANCO": item.get("bankTransfer"),
                    "Tipo de Tarjeta": item.get("cardType"),
                    "Marca tarjeta": item.get("brand"),
                    "Cuotas": 1,
                    "Moneda": item.get("currencyAlpha"),
                    "Monto": item.get("amount"),
                    "Monto Original": item.get("originalAmount"),
                    "Estado": item.get("stateDescription"),
                    "Info. transaccion": "",
                    "Fecha/hora de Registro": item.get("registryDate"),
                    "Fecha/hora de Pago": item.get("paymentDate"),
                    "Fecha de liquidacion": item.get("liquidationDate"),
                    "Fecha de compensacion": item.get("compensationDate"),
                    "Fee": item.get("fee"),
                    "Impuesto": item.get("tax"),
                    "Total Fee & Impuesto": item.get("sumFeeAndTAx"),
                    "Nombre Cliente": f"{item.get('customerName', '')} {item.get('customerLastName', '')}".strip(),
                    "Id Cliente Comercio": item.get("customerId"),
                    "Ret IVA": item.get("ivaWithhold"),
                    "Ret Deb&Cred": item.get("debCredWithhold"),
                    "Ret Profits": item.get("profitWithhold"),
                    "Total Retention": item.get("totalWithholds"),
                    "Cash transfer": item.get("otherCharges"),
                    "Monto a Liquidar": item.get("netMerchant"),
                    "Fecha de Contracargo": item.get("chargeBackDate"),
                    "Estado de la alerta": ""
                }
                rows.append(row)

            df = pd.DataFrame(rows)

            # Limpiar formatos de fecha
            df['Fecha/hora de Pago'] = df['Fecha/hora de Pago'].str.replace('T', ' ', regex=False)
            df['Fecha/hora de Registro'] = df['Fecha/hora de Registro'].str.replace('T', ' ', regex=False)

            # Orden final de columnas
            column_order = [
                "Origen", "Id Operacion", "Id Operacion Comercio", "Codigo de pago", "ID procesador", "Canal de Pago",
                "BANCO", "Tipo de Tarjeta", "Marca tarjeta", "Cuotas", "Moneda", "Monto", "Monto Original", "Estado",
                "Info. transaccion", "Fecha/hora de Registro", "Fecha/hora de Pago", "Fecha de liquidacion",
                "Fecha de compensacion", "Fee", "Impuesto", "Total Fee & Impuesto", "Nombre Cliente",
                "Id Cliente Comercio", "Ret IVA", "Ret Deb&Cred", "Ret Profits", "Total Retention",
                "Cash transfer", "Monto a Liquidar", "Fecha de Contracargo", "Estado de la alerta"
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
        print(f"[INICIO] Procesando Monnet desde {from_date} hasta {to_date}")
        
        # Obtener token del cache
        token = await token_cache_monnet.get_token()
        if not token:
            print("[ERROR] No se pudo obtener token de Monnet")
            return False

        # Descargar datos
        data_count, final_token = await get_data_json_monnet_async(token, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[INFO] {data_count} bytes descargados, generando archivo Excel")
            json_excel_monnet()
            print("[SUCCESS] Proceso Monnet completado exitosamente")
            return True
        else:
            print(f"[INFO] No hay datos para procesar (registros: {data_count})")
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    print(f"[WRAPPER] Ejecutando Monnet collector")
    return asyncio.run(get_data_main_async(from_date, to_date))

