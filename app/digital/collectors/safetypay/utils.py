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


# =============================
#   CACHE DE TOKEN SAFETYPAY
# =============================
class TokenCacheSafetyPay:
    def __init__(self):
        self.cookie_header = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_session_data(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.cookie_header and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token SafetyPay del cache")
                return self.cookie_header

            print("[INFO] Obteniendo nuevo token SafetyPay...")
            self.cookie_header = await get_token_safetypay()
            
            if self.cookie_header:
                # Token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token SafetyPay cacheado hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nuevo token SafetyPay")
            
            return self.cookie_header

    def invalidate(self):
        print("[INFO] Invalidando cookies SafetyPay cacheadas")
        self.cookie_header = None
        self.expires_at = None


token_cache_safetypay = TokenCacheSafetyPay()


# =============================
#   OBTENER TOKEN
# =============================
async def get_token_safetypay():
    
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para SafetyPay")
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
                viewport={"width": 1920, "height": 1080}
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)
            
            page = await context.new_page()

            try:
                print("[INFO] Navegando a SafetyPay")
                await page.goto("https://secure.safetypay.com/merchantportal/es/dashboard", wait_until="networkidle", timeout=60000)

                # Login
                print("[INFO] Realizando login")
                await page.click('.mp-btn.mp-btn_primary.mp-public-header__button')
                await page.fill('input[name="Username"], input[type="email"], input[type="text"]', Config.USER_NAME_SAFETYPAY)
                await page.fill('input[name="Password"], input[type="password"]', Config.PASSWORD_SAFETYPAY)
                await page.click('button[name="button"][value="login"]')
                
                # Esperar a que se complete el login
                print("[INFO] Esperando completar login")
                await page.wait_for_url("**/dashboard**", timeout=30000)
                
                # Navegar a la pagina de reportes para activar llamadas API
                print("[INFO] Navegando a pagina de reportes")
                try:
                    await page.goto("https://secure.safetypay.com/merchantportal/es/dashboard/reports/transactions", wait_until="networkidle", timeout=60000)
                    await asyncio.sleep(3)  
                except:
                    print("[INFO] No se pudo navegar a reportes, continuando")
                
                # Capturar cookies
                cookies = await context.cookies()
                cookie_header = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

                print(f"[INFO] Cookies capturadas: {cookie_header[:100]}...")

                if cookie_header:
                    print("[INFO] Cookies capturadas exitosamente")
                    return cookie_header 
                else:
                    print("[ERROR] No se pudieron capturar cookies")
                    return None
                    
            except Exception as e:
                print(f"[ERROR] Error durante la captura: {e}")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_safetypay: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_safetypay(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass



async def close_playwright_resources_safetypay(browser, context, page):
    print("[INFO] Cerrando recursos de SafetyPay...")
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
            print("[DEBUG] Recursos de SafetyPay cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de SafetyPay: {e}")


# =============================
#   FUNCIONES DE DATOS
# =============================
async def get_data_json_safetypay_async(cookie_header, from_date, to_date):
    
    start_date = from_date
    end_date = (to_date + timedelta(days=2))
        
    print(f"[INFO] Descargando transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")
    
    # Extraer X-XSRF-Token de las cookies
    xsrf_token = None
    if cookie_header:
        cookies_dict = {}
        for cookie in cookie_header.split('; '):
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                cookies_dict[key] = value
        
        # El token XSRF viene en la cookie .MP.Antiforgery
        xsrf_token = cookies_dict.get('.MP.Antiforgery', '')
        print(f"[INFO] X-XSRF-Token extraído: {xsrf_token[:50]}...")
    
    # Headers
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "es",
        "Connection": "keep-alive",
        "Cookie": cookie_header,
        "Host": "secure.safetypay.com",
        "merchantcodeid": '9006', 
        "merchantid": '2120',        
        "Referer": "https://secure.safetypay.com/merchantportal/es/dashboard/reports/transactions",
        "Sec-Ch-Ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "x-xsrf-token": xsrf_token
    }
    
    url = "https://secure.safetypay.com/MerchantPortalApi/v1/reports/account-activity/transactions"
    page_size = 2500
    all_data = []
    current_cookie_header = cookie_header
    current_xsrf_token = xsrf_token
    
    current = start_date
    while current < end_date:
        current_date_str = current.strftime("%m.%d.%Y")

        print(f"[INFO] Descargando transacciones del {current_date_str}")
    
        params = {
            "pageNumber": 1,
            "pageSize": page_size,
            "status": "ALL",
            "startDate": current_date_str,
            "endDate": current_date_str,
            "datePeriod": "Custom",
            "includeTrace": "true",  
            "salesCurrency": "PEN",
            "byCurrentBusinessAccount": "false"  
        }
        
        day_data = [] 
        
        # Crear session para mantener cookies y headers
        session = requests.Session()
        session.headers.update(headers)
        
        has_more_pages = True
        
        while has_more_pages:
            max_retries = 5
            success = False
            registros = []
            
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"[INFO] Solicitando pagina {params['pageNumber']} (Intento {attempt}/{max_retries})")
                    await asyncio.sleep(1) 
                    
                    response = session.get(url, params=params, timeout=30)
                    
                    print(f"[DEBUG] Status Code: {response.status_code}")
                    
                    if response.status_code == 200:
                        data = response.json()
                        if isinstance(data, dict) and "result" in data and "data" in data["result"]:
                            registros = data["result"]["data"]
                        else:
                            print("[INFO] No se encontro la clave 'result' o 'data' en la respuesta")
                            has_more_pages = False
                            success = True
                            break
                        
                        if not registros:
                            print("[INFO] No se encontraron mas registros")
                            has_more_pages = False
                            success = True
                            break
                        
                        day_data.extend(registros)
                        print(f"[INFO] Pagina {params['pageNumber']} - {len(registros)} registros obtenidos")

                        # Verificar si hay mas paginas
                        if page_size > len(registros):
                            print("[INFO] Ultima pagina alcanzada")
                            has_more_pages = False
                        else:
                            params["pageNumber"] += 1
                            
                        success = True
                        break
                    
                    elif response.status_code == 403:
                        print("[ERROR] Error 403: Acceso denegado")
                        print(f"[DEBUG] Response Content: {response.text[:500]}")
                        has_more_pages = False
                        break
                    
                    elif response.status_code in [401, 403]:
                        print(f"[ERROR] Error de autorizacion {response.status_code}")
                        if attempt < max_retries - 1:
                            # Renovar cookies y extraer nuevo XSRF token
                            new_cookie_header = await token_cache_safetypay.get_session_data(force_refresh=True)
                            if new_cookie_header:
                                current_cookie_header = new_cookie_header
                                
                                # Extraer nuevo XSRF token
                                cookies_dict = {}
                                for cookie in new_cookie_header.split('; '):
                                    if '=' in cookie:
                                        key, value = cookie.split('=', 1)
                                        cookies_dict[key] = value
                                current_xsrf_token = cookies_dict.get('.MP.Antiforgery', '')
                                
                                headers["Cookie"] = new_cookie_header
                                headers["x-xsrf-token"] = current_xsrf_token
                                session.headers.update(headers)
                                print("[INFO] Cookies y XSRF token SafetyPay renovados correctamente")
                            else:
                                print("[ERROR] No se pudieron renovar cookies SafetyPay")
                        break
                    
                    else:
                        print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)  
                        else:
                            has_more_pages = False
                        continue
                    
                except requests.exceptions.RequestException as e:
                    print(f"[ERROR] Error de conexion: {e} (intento {attempt}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)  
                    continue
                    
                except json.JSONDecodeError as e:
                    print(f"[ERROR] Error al decodificar JSON: {e}")
                    print(f"[DEBUG] Response content: {response.text[:500]}")
                    has_more_pages = False
                    break
                    
                except Exception as e:
                    print(f"[ERROR] Error inesperado: {e} (intento {attempt}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2) 
                    else:
                        has_more_pages = False
                    continue
                
            if not success:
                print(f"[ERROR] No se pudo obtener datos para pagina {params['pageNumber']}")
                has_more_pages = False
                break

        print(f"[INFO] Total registros para {current_date_str}: {len(day_data)}")
        all_data.extend(day_data)
        
        current += timedelta(days=1)
        
        # Pausa entre dias
        if current < end_date:
            await asyncio.sleep(2)
            
    session.close()
    print(f"[INFO] Total de registros obtenidos: {len(all_data)}")
    
    if all_data:
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        file_key = f"digital/collectors/safetypay/input/response_{current_time}.json"
        upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
        print(f"[SUCCESS] Archivo guardado en S3: {file_key}")

    return len(all_data), current_cookie_header


def json_excel_safetypay(from_date, to_date):
    
    start_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(minutes=1)
    end_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    from_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S") 
    to_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[INFO] Transformando transacciones del: {from_date_str} al {to_date_str}")
    
    prefix = "digital/collectors/safetypay/input/"
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
                data = data.get("Value") or data.get("data") or data.get("results") or []
            elif not isinstance(data, list):
                data = []

            if not data or not isinstance(data, list):
                print("[INFO] No hay datos validos para procesar")
                continue

            rows = []
            for item in data:
                row = {
                    "Estado": item.get("status", ""),
                    # ##"FECHA DE REGISTRO": (
                    #     pd.to_datetime(item.get("created"), errors="coerce") - timedelta(hours=5)
                    # ).strftime("%d/%m/%Y %H:%M:%S") if item.get("created") else ""
                    "Fecha": (
                        pd.to_datetime(item.get("date"), errors="coerce") - timedelta(hours=5)
                    ).strftime("%d/%m/%Y %H:%M:%S") if item.get("date") else "", 
                    "Id. de operación": item.get("operationId", ""),
                    "Id. de transacción": item.get("transactionId", ""),
                    "Id. de ventas de comerciantes": item.get("merchantReferenceNo", ""),
                    "Número de pedido del comerciante": item.get("merchantOrderNo", ""),
                    "Importe de la venta": item.get("salesAmount", ""),
                    "Moneda de venta": item.get("salesCurrency", ""),
                    "País de pago": item.get("paymentCountry", ""),
                    "Nombre del banco de pago": item.get("paymentBankName", ""),
                    "Moneda de pago": item.get("paymentCurrency", ""),
                    "Cantidad del comprador": item.get("shopperAmount", ""),
                    "Canal de pago": item.get("paymentChannel", ""),
                    "Creado por": item.get("createdBy", "")
                }
                rows.append(row)

            df = pd.DataFrame(rows)

            # Limpiar y transformar datos (manteniendo tildes en nombres de columnas)
            df['Fecha'] = df['Fecha'].str.replace('T', ' ', regex=False)
            df['Id. de transacción'] = df['Id. de transacción'].str.replace(' ', '', regex=False)
            df['Id. de ventas de comerciantes'] = (df['Id. de ventas de comerciantes']
                                    .str.replace('ATP-', '', regex=False)
                                    .str.replace('-', '.', regex=False)
                                    .astype(str))
            df['Número de pedido del comerciante'] = (df['Número de pedido del comerciante']
                                    .str.replace('ATP-', '', regex=False)
                                    .str.replace('-', '.', regex=False)
                                    .astype(str))

            # df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)

            column_order = [
                "Estado",
                "Fecha",
                "Id. de operación",
                "Id. de transacción",
                "Id. de ventas de comerciantes",
                "Número de pedido del comerciante",
                "Importe de la venta",
                "Moneda de venta",
                "País de pago",
                "Nombre del banco de pago",
                "Moneda de pago",
                "Cantidad del comprador",
                "Canal de pago",
                "Creado por"
            ]
            df = df[column_order]
            
            print(f"[INFO] Se procesaron {len(df)} registros")
            # Filtrar por fecha
            # df_filtrado = df[(df['Fecha'] >= from_date_str) & (df['Fecha'] <= to_date_str)]

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
        print(f"[INICIO] Procesando SafetyPay desde {from_date} hasta {to_date}")
        
        # Obtener cookies del cache
        cookie_header = await token_cache_safetypay.get_session_data()
        if not cookie_header:
            print("[ERROR] No se pudieron obtener cookies de SafetyPay")
            return False

        # Descargar datos
        data_count, final_cookie_header = await get_data_json_safetypay_async(cookie_header, from_date, to_date)
        
        if data_count and data_count > 0:
            print(f"[INFO] {data_count} registros descargados, generando archivo Excel")
            json_excel_safetypay(from_date, to_date)
            print("[SUCCESS] Proceso SafetyPay completado exitosamente")
            return True
        else:
            print("[INFO] No hay datos para procesar")
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_main_async: {e}")
        return False


def get_data_main(from_date, to_date):
    print(f"[WRAPPER] Ejecutando SafetyPay collector")
    return asyncio.run(get_data_main_async(from_date, to_date))


if __name__ == "__main__":
    asyncio.run(get_token_safetypay())
