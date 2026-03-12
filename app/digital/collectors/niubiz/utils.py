import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import requests
from datetime import datetime, timedelta
import json
import pytz
import time
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *


# =============================
#   CACHE DE TOKEN NIUBIZ
# =============================
class TokenCacheNiubiz:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False, type=1):
        # obtiene o renueva el token de niubiz usando get_token_niubiz_1
        async with self.lock:
            now = datetime.now()

            if (not force_refresh and self.token and
                    self.expires_at and now < self.expires_at):
                print("[info] usando token niubiz del cache")
                return self.token

            print("[info] obteniendo nuevo token niubiz...")
            self.token = await get_token_niubiz_1()

            if self.token:
                # token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[info] token niubiz cacheado hasta {self.expires_at}")
            else:
                print("[error] no se pudo obtener nuevo token niubiz")

            return self.token

    def invalidate(self):
        print("[info] invalidando token niubiz cacheado")
        self.token = None
        self.expires_at = None


token_cache_niubiz = TokenCacheNiubiz()


async def close_playwright_resources_niubiz(browser, context, page):
    print("[info] cerrando recursos de niubiz...")
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
            print("[debug] recursos de niubiz cerrados correctamente")
        except Exception as e:
            print(f"[warn] error cerrando recursos de niubiz: {e}")
        


# =============================
#   OPCION JSON 
# =============================
async def get_token_niubiz_1(max_login_attempts=3):
    print("[info] iniciando playwright para obtener token niubiz")
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[info] lanzando navegador chrome en modo headless")
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

            print("[info] inyectando script para ocultar webdriver")
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
                    print(f"[debug] token detectado: {token_found[:20]}...")
                await route.continue_()

            print("[info] interceptando todas las requests para buscar el token")
            await context.route("**", handle_request)

            print("[info] navegando a https://comercio.niubiz.com.pe")
            try:
                await page.goto("https://comercio.niubiz.com.pe", wait_until="networkidle", timeout=60000)
                print("[info] pagina cargada correctamente")
            except Exception as e:
                print(f"[error] error al cargar la pagina: {e}")
                return None
            
            login_attempt = 0
            while login_attempt < max_login_attempts:
                print(f"[info] intento de login #{login_attempt + 1}")
                
                # login (manteniendo la logica original que funciona)
                try:
                    await page.fill('input[name="username"], input[type="email"], input[type="text"]', Config.USER_NAME_NIUBIZ_2)
                    await page.fill('input[name="password"], input[type="password"]', Config.PASSWORD_NIUBIZ_2)
                    print("[info] haciendo click en el boton de login")
                    await page.click('button[type="submit"], #kc-login')
                except Exception as e:
                    print(f"[error] error durante el login: {e}")
                    break
                
                # esperar captura del token
                print("[info] esperando captura del token (30s max)")
                token_capturado = False
                for i in range(30):
                    if token_found:
                        print(f"[ok] token capturado en segundo {i + 1}")
                        token_capturado = True
                        break
                    await asyncio.sleep(1)
                    
                if token_capturado:
                    break
                
                # si no se obtuvo el token, refrescar y volver a intentar
                print("[info] token no detectado, refrescando pagina")
                await page.reload(wait_until="networkidle")
                login_attempt += 1
                
            if token_found:
                print("[info] token capturado exitosamente")
                return token_found
            else:
                print("[error] no se encontro token despues de {max_login_attempts} intentos")
                return None
                
    except Exception as e:
        print(f"[error] error general en get_token_niubiz: {e}")
        return None
        
    finally:
        # CIERRE GARANTIZADO de recursos
        await close_playwright_resources_niubiz(browser, context, page)




async def get_data_json_niubiz_async(token, from_date, to_date):
    # descarga el rango completo en una sola sesion paginando por numero de pagina
    from_date_str = f"{from_date.day}/{from_date.month}/{from_date.year}"
    to_date_str = f"{to_date.day}/{to_date.month}/{to_date.year}"

    print(f"[info] descargando rango completo desde {from_date_str} hasta {to_date_str}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token,
        "Origin": "https://comercio.niubiz.com.pe",
        "Referer": "https://comercio.niubiz.com.pe/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
    url = "https://api.niubiz.pe/api.backoffice.merchant/order"
    size = 50000
    all_data = []
    current_token = token
    page = 1
    has_more_pages = True
    last_record_ids = set()

    while has_more_pages:
        body = {
            "page": page,
            "size": size,
            "fromDate": from_date_str,
            "toDate": to_date_str,
            "merchantId": "650188903",
            "brand": [],
            "status": [],
            "currency": [],
            "transactionDate": "",
            "confirmationDate": "",
            "pagolinkId": None,
            "export": False
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
                        print(f"[info] no hay mas registros en pagina {page}")
                        has_more_pages = False
                        request_success = True
                        break

                    # detectar duplicados reales usando ids unicos
                    current_ids = {record.get('id') or record.get('purchaseNumber') for record in records if isinstance(record, dict)}

                    if current_ids and current_ids.issubset(last_record_ids):
                        print(f"[warn] registros duplicados detectados en pagina {page}, deteniendo")
                        has_more_pages = False
                        request_success = True
                        break

                    last_record_ids.update(current_ids)
                    all_data.extend(records)
                    print(f"[info] descargados {len(records)} registros (pagina {page}, total: {len(all_data)})")

                    if len(records) < size:
                        has_more_pages = False
                    else:
                        page += 1

                    request_success = True
                    break

                elif response.status_code == 401:
                    print(f"[error] error de autorizacion (401) en pagina {page}, intento {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        new_token = await token_cache_niubiz.get_token(force_refresh=True, type=1)
                        if new_token:
                            current_token = new_token
                            headers["Authorization"] = current_token
                            print("[info] token niubiz renovado correctamente")
                        else:
                            print("[warn] no se pudo renovar el token niubiz")
                            await asyncio.sleep(5)
                    else:
                        print(f"[error] fallo por error de autorizacion en pagina {page}")
                        has_more_pages = False
                        request_success = True

                elif response.status_code == 504:
                    print(f"[error] timeout (504) en pagina {page}, intento {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)
                    else:
                        print(f"[error] fallo por timeout en pagina {page}")
                        has_more_pages = False
                        request_success = True
                else:
                    print(f"[error] {response.status_code}: {response.text[:200]}")
                    has_more_pages = False
                    request_success = True
                    break

            except Exception as e:
                print(f"[error] excepcion en pagina {page}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                else:
                    has_more_pages = False
                    request_success = True
                    break

        if not request_success:
            print(f"[warn] no se proceso la pagina {page} correctamente")

        await asyncio.sleep(1)

    print(f"[info] total registros descargados: {len(all_data)}")

    if all_data:
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        file_key = f"digital/collectors/niubiz/input/response_{current_time}.json"
        upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
        print(f"[info] archivo guardado en s3: {file_key} con {len(all_data)} registros")

    return len(all_data)




async def get_data_main_json_async(from_date, to_date):
    try:
        print("[info] iniciando captura de token niubiz _ json")
        # corregido: await directo
        token = await token_cache_niubiz.get_token(type=1)
        
        if token:
            print("[info] trayendo datos de niubiz _ json")
            # corregido: await directo
            data_count = await get_data_json_niubiz_async(token, from_date, to_date)
            print(f"[debug] datos obtenidos: {data_count}")
            
            if data_count > 0:
                print("[info] generando archivo excel _ del json")
                processed_files = json_excel_niubiz()  
                return processed_files 
            else:
                print("[warn] no hay datos para procesar en json.")
                return []  
        else:
            print("[warn] no se pudo obtener el token de niubiz en json.")
            return []  

    except Exception as e:
        print(f"[error] error en obtener la data de niubiz json: {e}")
        return []


def get_data_main_json(from_date, to_date):
    # wrapper sincrono que ejecuta el proceso principal de niubiz y mide el tiempo de ejecucion
    start_time = time.time()

    print(f"\n{'='*50}")
    print(f"[inicio] proceso niubiz | rango: {from_date.date()} a {to_date.date()}")
    print(f"{'='*50}\n")

    try:
        result = asyncio.run(get_data_main_json_async(from_date, to_date))
    except Exception as e:
        print(f"[error] fallo ejecucion principal niubiz: {e}")
        result = []
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"\n{'='*50}")
        print(f"[fin] proceso niubiz completado")
        print(f"[tiempo] duracion total: {elapsed_time / 60:.2f} minutos")
        print(f"{'='*50}\n")

    return result


def json_excel_niubiz():
    prefix = "digital/collectors/niubiz/input/"
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


        # mover el .json a input/processed/
        processed_key = file_key.replace("input/", "input/processed/", 1)
        upload_file_to_s3(content, processed_key)
        delete_file_from_s3(file_key)
        
        processed_files.append(output_key)
        
        ##download_file_from_s3_to_local(output_key)##solo para pruebitas
        print(f"[info] procesado: {file_key} -> {output_key} y movido a {processed_key}")

    print("[ok] proceso json -> excel completado.")
    return processed_files 



