import asyncio
from playwright.async_api import async_playwright
import requests
from datetime import datetime, timedelta
import pytz
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *
import math
import time
import json
import pandas as pd

# =============================
#   CACHE DE TOKEN PAGOEFECTIVO
# =============================
class TokenCachePagoEfectivo:
    def __init__(self):
        self.token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def get_token(self, force_refresh=False):
        async with self.lock:
            now = datetime.now()
            
            if (not force_refresh and self.token and 
                self.expires_at and now < self.expires_at):
                print("[INFO] Usando token PagoEfectivo del cache")
                return self.token

            print("[INFO] Obteniendo nuevo token PagoEfectivo...")
            self.token = await get_token_pagoefectivo()
            
            if self.token:
                # Token valido por 30 minutos
                self.expires_at = now + timedelta(minutes=30)
                print(f"[INFO] Token PagoEfectivo cacheado hasta {self.expires_at}")
            else:
                print("[ERROR] No se pudo obtener nuevo token PagoEfectivo")
            
            return self.token

    def invalidate(self):
        print("[INFO] Invalidando token PagoEfectivo cacheado")
        self.token = None
        self.expires_at = None


token_cache_pagoefectivo = TokenCachePagoEfectivo()


# =============================
#   OBTENER TOKEN 
# =============================
async def get_token_pagoefectivo():
    browser = None
    context = None
    page = None
    
    try:
        async with async_playwright() as p:
            print("[INFO] Lanzando navegador para PagoEfectivo")
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
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                ignore_https_errors=True
            )

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page = await context.new_page()
            token_found = None

            def handle_request(request):
                nonlocal token_found
                headers = request.headers
                # Capturar token en cualquier URL que contenga 'userinfo'
                if 'authorization' in headers and not token_found and 'userinfo' in request.url:
                    auth_header = headers['authorization']
                    if auth_header.startswith('Bearer '):
                        token_found = auth_header.split(' ', 1)[1]
                    else:
                        token_found = auth_header
                    print(f"[DEBUG] Token capturado: {token_found[:20]}...")

            page.on('request', handle_request)

            print("[INFO] Navegando a PagoEfectivo")
            await page.goto("https://admin.pagoefectivolatam.com/", wait_until="networkidle", timeout=60000)

            # Login
            print("[INFO] Realizando login")
            await page.click('.pagoefectivo-button')
            await page.fill('input[name="Username"], input[type="email"], input[type="text"]', Config.USER_NAME_PAGOEFECTIVO)
            await page.fill('input[name="Password"], input[type="password"]', Config.PASSWORD_PAGOEFECTIVO)
            await page.click('button[name="button"][value="login"]')

            # Esperar captura del token
            print("[INFO] Esperando captura del token (30s max)")
            for i in range(30):
                if token_found:
                    print(f"[SUCCESS] Token capturado en segundo {i + 1}")
                    break
                await asyncio.sleep(1)

            if token_found:
                print("[✔] Token capturado exitosamente.")
                return token_found
            else:
                print("[✖] No se encontro token despues de 30 segundos.")
                return None
                
    except Exception as e:
        print(f"[ERROR] Error general en get_token_pagoefectivo: {e}")
        return None
        
    finally:
        # Cierre GARANTIZADO de recursos
        await close_playwright_resources_pagoefectivo(browser, context, page)
        
        try:
            await p.__aexit__(None, None, None)  
            print("[DEBUG] Playwright cerrado completamente.")
        except Exception:
            pass


async def close_playwright_resources_pagoefectivo(browser, context, page):
    print("[INFO] Cerrando recursos de PagoEfectivo...")
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
            print("[DEBUG] Recursos de PagoEfectivo cerrados correctamente")
        except Exception as e:
            print(f"[WARN] Error cerrando recursos de PagoEfectivo: {e}")


# =============================
#   FUNCIONES DE DATOS
# =============================
async def get_data_pagoefectivo_async(bearer, from_date, to_date):
    start_date = from_date
    end_date = to_date
        
    print(f"[INFO] Enviando solicitudes para transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }

    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/reports/histcippro?"
    
    current = start_date
    request_count = 0
    current_token = bearer
    
    while current <= end_date:
        from_d = current.strftime("%Y-%m-%d")
        to_d = current.strftime("%Y-%m-%d")

        print(f"[INFO] Enviando solicitud para transacciones del {from_d}")

        body = {
            "NumeroOrdenPago":"",
            "IdEmpresa":0,
            "IdServicio":0,
            "IdUsuario":0,
            "UsuarioNombre":"",
            "IdTipoOrigenCancelacion":0,
            "IdEstado":0,
            "FechaInicio":from_d,
            "FechaFin":to_d,
            "ClienteEmail":"",
            "OrderIdComercio":"",
            "IdUsuarioRepresentante":25980688,
            "PageNumber":1,
            "PageSize":10,
            "PropOrder":"",
            "TipoOrder":1,
            "NombreServicio":"Todos"
        }
        
        max_retries = 3
        success = False
        
        for retry in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=120)
                if response.status_code == 200:
                    print(f"[SUCCESS] Solicitud enviada para {from_d}")
                    request_count += 1
                    success = True
                    break
                    
                elif response.status_code == 401:
                    print(f"[ERROR] Token expirado (401) para {from_d}")
                    if retry < max_retries - 1:
                        print("[INFO] Renovando token...")
                        new_token = await token_cache_pagoefectivo.get_token(force_refresh=True)
                        if new_token:
                            current_token = new_token
                            headers["Authorization"] = current_token
                            print("[INFO] Token renovado correctamente")
                        else:
                            print("[ERROR] No se pudo renovar el token")
                    break
                    
                else:
                    print(f"[ERROR] Status {response.status_code} para {from_d}: {response.text[:200]}")
                    if retry < max_retries - 1:
                        await asyncio.sleep(5)
                    
            except Exception as e:
                print(f"[ERROR] Error enviando solicitud para {from_d}: {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)

        if not success:
            print(f"[WARN] No se pudo enviar solicitud para {from_d}, continuando...")

        # Esperar entre solicitudes
        if current < end_date:
            print("[INFO] Esperando 10 segundos antes de la siguiente solicitud")
            await asyncio.sleep(10)
            
        current += timedelta(days=1)
    
    print(f"[INFO] Total de solicitudes enviadas: {request_count}")
    return request_count, current_token


async def get_routes_pagoefectivo_async(bearer, request_count, max_attempts=60, wait_seconds=120):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/transfers/monitorquery?"

    lima_tz = pytz.timezone("America/Lima")
    today = datetime.now(lima_tz).strftime("%Y-%m-%d")
    from_d = today
    to_d = today
    
    body = {
        "TipoReporte": 1117,
        "FechaInicio": from_d,
        "FechaFin": to_d,
        "CodUsuario": "BD30546B-F192-4493-8500-69C08E610FAB"
    }

    target_reports = []
    current_token = bearer
    
    for attempt in range(max_attempts):
        try:
            print(f"[INFO] Intento {attempt + 1}/{max_attempts} para obtener y monitorear reportes")
            
            response = requests.post(url, headers=headers, json=body, timeout=120)
            
            if response.status_code == 200:
                data = response.json()
                
                # Ordenar todos los reportes por fecha (mas recientes primero)
                all_reports = sorted(data.get("Data", []), 
                                   key=lambda x: x.get("FechaExportacion", ""), 
                                   reverse=True)
                
                # Tomar los N mas recientes
                latest_reports = all_reports[:request_count]
                
                # Si es el primer intento, identificar que reportes vamos a monitorear
                if attempt == 0:
                    target_reports = [report["IdReporte"] for report in latest_reports if "IdReporte" in report]
                    print(f"[INFO] Monitoreando los {len(target_reports)} reportes mas recientes:")
                    for i, report in enumerate(latest_reports, 1):
                        estado = report.get('Estado', 'Desconocido')
                        fecha = report.get('FechaExportacion', 'Sin fecha')
                        print(f"  {i}. ID: {report.get('IdReporte', 'N/A')}, Estado: {estado}, Fecha: {fecha}")
                
                # Verificar el estado de los reportes objetivo
                completed_reports = []
                for report in data.get("Data", []):
                    if (report.get("IdReporte") in target_reports and 
                        report.get("Estado") == "Completado"):
                        completed_reports.append(report)
                
                print(f"[INFO] Reportes completados: {len(completed_reports)}/{len(target_reports)}")
                
                # Si todos los reportes objetivo estan completados, proceder
                if len(completed_reports) >= len(target_reports):
                    print(f"[SUCCESS] Todos los {len(target_reports)} reportes estan completados")
                    
                    # Ordenar los completados por fecha y tomar los solicitados
                    completed_reports_sorted = sorted(completed_reports, 
                                                    key=lambda x: x.get("FechaExportacion", ""), 
                                                    reverse=True)[:request_count]
                    
                    # Extraer rutas y nombres
                    rutas = [report.get("RutaArchivo", "") for report in completed_reports_sorted]
                    nombres = [report.get("NombreArchivo", "") for report in completed_reports_sorted]
                    idreporte = [report.get("IdReporte", "") for report in completed_reports_sorted]
                    
                    print(f"[INFO] {len(rutas)} rutas obtenidas")
                    return rutas, nombres, idreporte, current_token
                
                # Si no estan todos completados, esperar y reintentar
                if attempt < max_attempts - 1:
                    print(f"[INFO] Esperando {wait_seconds} segundos antes del proximo intento...")
                    await asyncio.sleep(wait_seconds)
                    
            elif response.status_code == 401:
                print(f"[ERROR] Token expirado (401) en monitoreo")
                if attempt < max_attempts - 1:
                    new_token = await token_cache_pagoefectivo.get_token(force_refresh=True)
                    if new_token:
                        current_token = new_token
                        headers["Authorization"] = current_token
                        print("[INFO] Token renovado para monitoreo")
                    else:
                        print("[ERROR] No se pudo renovar token para monitoreo")
                await asyncio.sleep(5)
                    
            else:
                print(f"[ERROR] Status {response.status_code} en monitoreo: {response.text[:200]}")
                
        except Exception as e:
            print(f"[ERROR] Excepcion durante monitoreo: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(5)
    
    # Devolver los reportes que si esten completados
    print(f"[WARN] No se completaron todos los reportes despues de {max_attempts} intentos")
    try:
        response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 200:
            data = response.json()
            completed_reports = [report for report in data.get("Data", []) 
                               if report.get("IdReporte") in target_reports and 
                               report.get("Estado") == "Completado"]
            
            completed_reports_sorted = sorted(completed_reports, 
                                            key=lambda x: x.get("FechaExportacion", ""), 
                                            reverse=True)[:request_count]
            
            rutas = [report.get("RutaArchivo", "") for report in completed_reports_sorted]
            nombres = [report.get("NombreArchivo", "") for report in completed_reports_sorted]
            idreporte = [report.get("IdReporte", "") for report in completed_reports_sorted]
            
            print(f"[INFO] Devolviendo {len(rutas)} reportes completados")
            return rutas, nombres, idreporte, current_token
    except Exception as e:
        print(f"[ERROR] Error al obtener reportes completados finales: {e}")
    
    return [], [], current_token


def get_download_files_pagoefectivo(bearer, idreporte, nombre_archivo):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/paiddownload"

    params = {
        "IdReporte": idreporte
    }

    try:
        print(f"[INFO] Descargando archivo: {nombre_archivo}")
        response = requests.get(url, headers=headers, params=params, timeout=120)
        
        if response.status_code == 200:
            response_text = response.text
            
            if response_text.startswith(('http://', 'https://')):
                file_resp = requests.get(response_text, timeout=500)

                if file_resp.status_code == 200:
                    file_content = file_resp.content
                    
                    # Validar que sea un archivo Excel
                    if len(file_content) > 1000:  # Minimo tamaño razonable para Excel
                        nombre_base = nombre_archivo.replace('.xlsx', '').replace('ó', 'o')
                        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
                        output_key = f"digital/collectors/pagoefectivo/input/{nombre_base}_{current_time}.xlsx"
                        
                        with BytesIO(file_content) as buffer:
                            upload_file_to_s3(buffer.getvalue(), output_key)
                        
                        print(f"[SUCCESS] Archivo guardado: {output_key}")
                        return output_key
                    else:
                        print(f"[ERROR] Archivo demasiado pequeño o invalido: {len(file_content)} bytes")
                        return None
                else:
                    print(f"[ERROR] Error al descargar archivo: {file_resp.status_code}")
                    return None
            else:
                print(f"[ERROR] Respuesta inesperada del servidor: {response_text[:200]}")
                return None
            
        else:
            print(f"[ERROR] Status {response.status_code}: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"[ERROR] Excepcion durante descarga: {e}")
        return None


# =============================
#   FUNCION PRINCIPAL
# =============================
async def get_main_pagoefectivo_async(from_date, to_date):
    try:
        print(f"[INICIO] Procesando PagoEfectivo desde {from_date} hasta {to_date}")
        
        # Obtener token del cache
        bearer = await token_cache_pagoefectivo.get_token()
        if not bearer:
            print("[ERROR] No se pudo obtener token de PagoEfectivo")
            return None

        # Enviar solicitudes de reportes
        request_count, updated_token = await get_data_pagoefectivo_async(bearer, from_date, to_date)
        
        if request_count > 0:
            print(f"[INFO] {request_count} solicitudes enviadas, monitoreando reportes...")
            
            # Monitorear y obtener rutas de reportes
            rutas, nombres, idreporte, final_token = await get_routes_pagoefectivo_async(updated_token, request_count)
            
            if rutas and nombres:
                downloaded_files = []
                print(f"[INFO] Descargando {len(rutas)} archivos...")
                
                for i, (ruta, nombre, idrep) in enumerate(zip(rutas, nombres, idreporte), 1):
                    print(f"[INFO] Descargando {i}/{len(rutas)}: {nombre}")
                    
                    file_path = get_download_files_pagoefectivo(final_token, idrep, nombre)
                    if file_path:
                        downloaded_files.append(file_path)
                    else:
                        print(f"[WARN] No se pudo descargar: {nombre}")
                    
                    # Pequeña pausa entre descargas
                    if i < len(rutas):
                        await asyncio.sleep(2)
                
                print(f"[SUCCESS] Proceso completado. {len(downloaded_files)} archivos descargados")
                return downloaded_files
            else:
                print("[ERROR] No se obtuvieron rutas de archivos")
                return None
        else:
            print("[INFO] No se enviaron solicitudes de reporte")
            return None
        
    except Exception as e:
        print(f"[ERROR] Error en get_main_pagoefectivo_async: {e}")
        return None


def get_main_pagoefectivo(from_date, to_date):
    print(f"[WRAPPER] Ejecutando PagoEfectivo collector")
    return asyncio.run(get_main_pagoefectivo_async(from_date, to_date))


# =============================
#   FUNCION DE UTILIDAD
# =============================
def save_dfs_to_excel(writer, dfs_dict, chunk_size=1_000_000):
    for sheet_name, df in dfs_dict.items():
        num_chunks = math.ceil(len(df) / chunk_size) if len(df) > 0 else 1
        for i in range(num_chunks):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            chunk = df.iloc[start:end]
            final_sheet_name = f"{sheet_name}_{i+1}" if num_chunks > 1 else sheet_name
            chunk.to_excel(writer, sheet_name=final_sheet_name, index=False)
        
        
##### OPCION DE EXCEL SI FALLA DESPUES LA SOLICITUD ##

def get_data_json_pagoefectivo(token, from_date, to_date):
    # convertir fechas si vienen como string
    if isinstance(from_date, str):
        start_date = datetime.strptime(from_date, '%Y-%m-%d').date()
    else:
        start_date = from_date
        
    if isinstance(to_date, str):
        end_date = datetime.strptime(to_date, '%Y-%m-%d').date()
    else:
        end_date = to_date
        
    print(f"[INFO] Descargando transacciones del: {start_date.strftime('%Y-%m-%d')} al {end_date.strftime('%Y-%m-%d')}")

    headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": token
        }

    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/histcippaid"
    size = 2500  
    all_data = []

    current = start_date
    while current <= end_date:
        current_date_str = current.strftime("%Y-%m-%d")

        print(f"[INFO] Descargando transacciones del {current_date_str}")

        page = 1
        day_data = []  
        has_more_pages = True
        
        while has_more_pages:

            body = {
                "NumeroOrdenPago":"",
                "IdEmpresa":0,
                "IdServicio":0,
                "CodUsuario":"BD30546B-F192-4493-8500-69C08E610FAB",
                "IdTipoOrigenCancelacion":0,
                "IdEstado":0,
                "FechaInicio":current_date_str,
                "FechaFin":current_date_str,
                "ClienteEmail":"",
                "OrderIdComercio":"",
                "IdUsuarioRepresentante":25980688,
                "PageNumber":page,
                "PageSize":size,
                "PropOrder":"",
                "TipoOrder":1,
                "NombreServicio":"Todos",
                "UsuarioNombre":""
            }
            max_retries = 5
            records = []
            
            for attempt in range(max_retries):
                try:
                    response = requests.post(url, headers=headers, json=body, timeout=120)
                    if response.status_code == 200:
                        data = response.json()
                        print(f"[DEBUG] Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}")
                        
                        if isinstance(data, dict) and "Value" in data:
                            records = data["Value"]
                        elif isinstance(data, dict) and "IsSuccess" in data and data["IsSuccess"]:
                            records = data.get("Value", [])
                        else:
                            records = data if isinstance(data, list) else []
                            
                        if not records:
                            print(f"[ALERTA] No hay registros para {current_date_str}, pagina {page}")
                            has_more_pages = False
                            break
                            
                        day_data.extend(records)
                        print(f"[INFO] Descargados {len(records)} registros (pagina {page})")
                        
                        # verificar si hay mas paginas
                        if len(records) < size:
                            has_more_pages = False
                        else:
                            page += 1
                            
                        break  # adios
                        
                    elif response.status_code == 504:
                        print(f"[ALERTA] Timeout (504) en pagina {page}. Intento {attempt + 1}/{max_retries}")
                        if attempt < max_retries - 1:
                            time.sleep(5)
                        else:
                            print(f"[ALERTA] Fallo por timeout en pagina {page}")
                            has_more_pages = False
                    elif response.status_code == 401:
                        print(f"[ALERTA] Error de autorizacion (401) en pagina {page}. Intento {attempt + 1}/{max_retries}")
                        if attempt < max_retries - 1:
                            print("[INFO] Intentando renovar token Pagoefectivo...")
                            token = asyncio.run(get_token_pagoefectivo())
                            if token:
                                headers["Authorization"] = token
                                print("[INFO] Token Pagoefectivo renovado correctamente.")
                            else:
                                print("[ALERTA] No se pudo renovar el token Pagoefectivo.")
                            time.sleep(5)
                        else:
                            print(f"[ALERTA] Fallo por error de autorizacion en pagina {page}")
                            has_more_pages = False
                    else:
                        print(f"[ALERTA] Error {response.status_code}: {response.text[:500]}")
                        has_more_pages = False
                        break
                        
                except Exception as e:
                    print(f"[ALERTA] ExcepciOn en pagina {page}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(5)
                    else:
                        has_more_pages = False
                        break
            
            # pausa
            time.sleep(1)

        print(f"[INFO] Total registros para {current_date_str}: {len(day_data)}")
        all_data.extend(day_data)
        current += timedelta(days=1)
        
    if all_data:
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        file_key = f"digital/collectors/pagoefectivo/input/response_{current_time}.json"

        upload_file_to_s3(json.dumps(all_data, ensure_ascii=False).encode("utf-8"), file_key)
        # download_file_from_s3_to_local(file_key)  # pruebitas

    return len(all_data)


def json_excel_pagoefectivo():
    prefix = "digital/collectors/pagoefectivo/input/"
    files = list_files_in_s3(prefix)

    for file_key in files:
        if not file_key.endswith(".json") or "/input/processed/" in file_key:
            continue

        print(f"[INFO] Procesando {file_key}")
        content = read_file_from_s3(file_key)
        data = json.loads(content.decode("utf-8"))

        if isinstance(data, dict):
            data = data.get("Value") or data.get("data") or data.get("results") or []
        elif not isinstance(data, list):
            data = []

        if not data or not isinstance(data, list):
            print("[ALERTA] No hay datos validos para procesar.")
            continue

        rows = []
        for item in data:
            
            row = {
                "CIP": item.get("NumeroOrdenPago", ""),
                "Nro.Ord.Comercio": item.get("OrderIdComercio", ""),
                "Monto": item.get("Total", 0),
                "Estado": item.get("DescripcionEstado", ""),
                "Fec.Emisión": item.get("FechaEmision", ""),
                "Fec.Cancelación": item.get("FechaCancelacion", ""),
                "Fec.Anulada": item.get("FechaAnulada", ""),
                "FechaAExpirar": item.get("FechaExpirada", ""),
                "Fec.Eliminado": item.get("FechaEliminado", ""),
                "Servicio": item.get("DescripcionServicio", ""),
                "Cliente Nombre": item.get("ClienteNombre", ""),
                "Cliente Apellidos": item.get("ClienteApellidos", ""),
                "Cliente Email": item.get("ClienteEmail", ""),
                "Tipo Doc.": item.get("ClienteTipoDocumento", ""),
                "Nro Documento": item.get("ClienteNroDocumento", ""),
                "Cliente Alias": item.get("ClienteAlias", ""),
                "Cliente Telefono": item.get("ClienteTelefono", ""),
                "Concepto Pago": item.get("ConceptoPago", ""),
                "Datos Adicionales": item.get("ConceptoPago", ""),
                "Canal": item.get("CanalPago", ""),
                "Fec.Actualizacion" : item.get("FechaActualizacion", "")
                
            }

            rows.append(row)

        df = pd.DataFrame(rows)

        # Orden final de columnas
        column_order = [
            "CIP",
            "Nro.Ord.Comercio",
            "Monto",
            "Estado",
            "Fec.Emisión",
            "Fec.Cancelación",
            "Fec.Anulada",
            "FechaAExpirar",
            "Fec.Eliminado",
            "Servicio",
            "Cliente Nombre",
            "Cliente Apellidos",
            "Cliente Email",
            "Tipo Doc.",
            "Nro Documento",
            "Cliente Alias",
            "Cliente Telefono",
            "Concepto Pago",
            "Datos Adicionales",
            "Canal",
            "Fec.Actualizacion"
        ]
        df = df[column_order]
        
        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            output_key = file_key.replace(".json", ".csv")
            upload_file_to_s3(buffer.getvalue(), output_key)

        # download_file_from_s3_to_local(output_key)

        # Mover el .json a input/processed/
        processed_key = file_key.replace("input/", "input/processed/", 1)
        upload_file_to_s3(content, processed_key)
        delete_file_from_s3(file_key)

        print(f"[INFO] Procesado: {file_key} -> {output_key} y movido a {processed_key}")

    print("[✔] Proceso Json -> Excel completado.")


def get_data_main_json(from_date, to_date):
    try:
        print("[INFO] Iniciando captura de token PagoEfectivo")
        token = asyncio.run(get_token_pagoefectivo())
        if token:
            print("[INFO] Trayendo datos de PagoEfectivo")
            data = get_data_json_pagoefectivo(token, from_date, to_date)
            print(f"[DEBUG] Datos obtenidos: {data}")
            if data > 0:
                print("[INFO] Generando archivo Excel")
                json_excel_pagoefectivo()
                return True
            else:
                print("[ALERTA] No hay datos para procesar.")
                return False
        else:       
            print("[ALERTA] No se pudo obtener el token de PagoEfectivo.")
            return False
    except Exception as e:
            print(f"[✖] Error en obtener la data de PagoEfectivo: {e}")
            return False


