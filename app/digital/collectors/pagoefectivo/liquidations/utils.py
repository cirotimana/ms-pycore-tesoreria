from app.digital.collectors.pagoefectivo.utils import *
import pandas as pd
import asyncio
import requests
from datetime import datetime, timedelta
import pytz
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *
import re
import numpy as np

# =============================
#   FUNCIONES ASINCRONAS
# =============================

async def get_id_async(bearer, from_date, to_date): 
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/transfers/processquery?"

    body = {
        "IdUser": 25980688,
        "IdService": -1,
        "IdCoin": -1,
        "IdTransferStatus": "4",
        "DateIniTransfer": from_date,
        "DateEndTransfer": to_date,
        "OrderBy": "DateRegistrationTransfer",
        "IsAccending": "false",
        "PageNumber": 1,
        "PageSize": 20
    }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 200:
            data = response.json()

            idtransfer = []
            service = []
            
            for record in data["Value"]["Transfers"]:
                idtransfer.append(record["IdTransfer"])
                service.append(record["Service"])
           
            print(f"[INFO] IDs encontrados: {idtransfer}")
            print(f"[INFO] Services encontradas: {service}")
            return idtransfer, service
        else:
            print(f"[✖] Error {response.status_code}: {response.text}")
            return [], []
    except Exception as e:
        print(f"[✖] Excepcion durante la llamada: {e}")
        return [], []

async def get_request_download_async(bearer, idtransfer, service):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/transfers/report?"

    body = {
        "IdTipoArchivo": 2,
        "IdUsuario": 25980688,
        "Moneda": "Soles",
        "NroTransferencia": idtransfer,
        "Servicio": service
    }

    try:
        response = requests.post(url, headers=headers, json=body, timeout=120)
        if response.status_code == 200:
            print(f"[INFO] Se envio solicitud del idtransfer: {idtransfer} - servicio {service}")
            return True
        else:
            print(f"[✖] Error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"[✖] Excepcion durante la llamada: {e}")
        return False

async def get_routes_liq_async(bearer, target_ids, from_d, to_d, max_attempts=25, wait_seconds=60):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/transfers/monitorquery?"

    body = {
        "TipoReporte": 1118,
        "FechaInicio": from_d,
        "FechaFin": to_d,
        "CodUsuario": "BD30546B-F192-4493-8500-69C08E610FAB"
    }

    completed_files = {}
    
    for attempt in range(max_attempts):
        try:
            print(f"[INFO] Intento {attempt + 1}/{max_attempts} para obtener rutas completadas")
            
            response = requests.post(url, headers=headers, json=body, timeout=120)
            if response.status_code == 200:
                data = response.json()
                print(f"[DEBUG] Response data count: {len(data.get('Data', []))}")
                
                for item in data["Data"]:
                    idtransfer = item["IdTransferencia"]
                    estado = item["Estado"]
                    print(f"[DEBUG] Procesando: IdTransferencia={idtransfer}, Estado={estado}, IdReporte={item.get('IdReporte', 'NO EXISTE')}")
                    
                    if idtransfer in target_ids and estado == "Completado":
                        tipo_archivo = item["NombreArchivo"].lower()

                        if not tipo_archivo.endswith(".txt"):
                            continue

                        new_date = datetime.fromisoformat(item["FechaExportacion"].replace('Z', '+00:00'))

                        if idtransfer not in completed_files:
                            completed_files[idtransfer] = {
                                "id_reporte": item["IdReporte"],
                                "nombre": item["NombreArchivo"],
                                "referencia": idtransfer,
                                "fecha_exportacion": item["FechaExportacion"]
                            }
                        else:
                            current_date = datetime.fromisoformat(completed_files[idtransfer]["fecha_exportacion"].replace('Z', '+00:00'))
                            if new_date > current_date:
                                completed_files[idtransfer] = {
                                    "id_reporte": item["IdReporte"],
                                    "nombre": item["NombreArchivo"],
                                    "referencia": idtransfer,
                                    "fecha_exportacion": item["FechaExportacion"]
                                }

                if len(completed_files) == len(target_ids):
                    print(f"[INFO] Todos los IDs ({len(target_ids)}) estan completados")
                    break
                else:
                    missing_ids = set(target_ids) - set(completed_files.keys())
                    print(f"[INFO] Faltan completar {len(missing_ids)} IDs: {missing_ids}")
                    
                    if attempt < max_attempts - 1:
                        print(f"[INFO] Esperando {wait_seconds} segundos antes del proximo intento...")
                        await asyncio.sleep(wait_seconds)
                    
            else:
                print(f"[✖] Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"[✖] Excepcion durante la llamada: {e}")
    
    rutas = []
    referencias = []
    nombres = []
    
    for idtransfer in target_ids:
        if idtransfer in completed_files:
            rutas.append(completed_files[idtransfer]["id_reporte"])
            referencias.append(completed_files[idtransfer]["referencia"])
            nombres.append(completed_files[idtransfer]["nombre"])
        else:
            print(f"[ALERTA] ID {idtransfer} no completado despues de {max_attempts} intentos")
            rutas.append(None)
            referencias.append(idtransfer)
            nombres.append(None)
    
    return rutas, referencias, nombres

async def get_download_files_async(bearer, id_reporte, referencia, nombre_archivo):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": bearer
    }
    
    url = "https://siyxkac92h.execute-api.us-east-1.amazonaws.com/v1/adm/cips/paiddownload"

    params = {
        "IdReporte": id_reporte
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        if response.status_code == 200:
            response_text = response.text
            
            if response_text.startswith(('http://', 'https://')):
                print(f"URL de descarga: {response_text}")
                
                file_resp = requests.get(response_text, timeout=500)

                if file_resp.status_code == 200:
                    bytes = file_resp.content
                    with BytesIO(bytes) as buffer:
                        nombre_base = nombre_archivo.replace('.txt', '').replace('.xls', '')
                        output_key = f"digital/collectors/pagoefectivo/liquidations/{referencia}_{nombre_base}.txt"
                        upload_file_to_s3(buffer.getvalue(), output_key)
                    return output_key
                else:
                    print(f"[✖] Error al descargar archivo real: {file_resp.status_code}")
                    return None
            else:
                print(f"Respuesta inesperada: {response_text}")
                return None
            
        else:
            print(f"[✖] Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"[✖] Excepcion durante la llamada: {e}")
        return None

async def get_data_liq_async(bearer, from_date, to_date):
    try:        
        start_date = from_date
        end_date = to_date + timedelta(days=7)    

        from_dt = start_date.strftime("%Y-%m-%d")
        to_dt = end_date.strftime("%Y-%m-%d")
        
        print(f"[INFO] descargando liquidaciones del: {from_dt} al {to_dt}")
        
        ids, services = await get_id_async(bearer, from_dt, to_dt)
        print(f"[INFO] las referencias descargadas son: {ids}")
        
        # Ejecutar todas las solicitudes de descarga en paralelo
        tasks = []
        for id, service in zip(ids, services):
            task = get_request_download_async(bearer, id, service)
            tasks.append(task)
        
        # Esperar a que todas las solicitudes terminen
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[ALERTA] Error en solicitud para idtransfer {ids[i]}: {result}")
            elif not result:
                print(f"[ALERTA] No se pudo enviar solicitud para idtransfer: {ids[i]}")
            
        return ids
            
    except Exception as e:
        print(f"[ERROR] Error en el proceso get_data_liq: {e}")
        return []

async def get_data_files_all_async(bearer, from_date, to_date):
    try:
        # Paso 1: Obtener IDs y enviar solicitudes de descarga
        print("[PASO 1] Obteniendo IDs y enviando solicitudes de descarga...")
        target_ids = await get_data_liq_async(bearer, from_date, to_date)
        
        if not target_ids:
            print("[INFO] No se encontraron IDs para procesar")
            return []
        
        # Paso 2: Convertir date_param al formato para get_routes_liq
        lima_tz = pytz.timezone("America/Lima")
        today = datetime.now(lima_tz).strftime("%Y-%m-%d")
        from_d = today
        to_d = today
        
        print(f"[INFO] Consultando rutas para fecha: {from_d}")
        
        # Paso 3: Obtener rutas de archivos completados
        print("[PASO 2] Obteniendo rutas de archivos completados...")
        rutas, referencias, nombres = await get_routes_liq_async(bearer, target_ids, from_d, to_d)
        
        # Paso 4: Descargar archivos en paralelo
        print("[PASO 3] Descargando archivos...")
        download_tasks = []
        for id_reporte, referencia, nombre in zip(rutas, referencias, nombres):
            print(f"[DEBUG] Verificando: id_reporte={id_reporte}, referencia={referencia}, nombre={nombre}")
            if id_reporte and nombre:
                print(f"[INFO] Programando descarga para referencia {referencia}: {nombre}")
                task = get_download_files_async(bearer, id_reporte, referencia, nombre)
                download_tasks.append(task)
            else:
                print(f"[WARN] Saltando descarga para referencia {referencia}: id_reporte={id_reporte}, nombre={nombre}")
        
        # Ejecutar todas las descargas en paralelo
        downloaded_files = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Filtrar resultados exitosos
        successful_downloads = []
        for result in downloaded_files:
            if result and not isinstance(result, Exception):
                successful_downloads.append(result)
        
        print(f"[INFO] Proceso completado. Archivos descargados: {len(successful_downloads)}")
        return successful_downloads
        
    except Exception as e:
        print(f"[ERROR] Error en get_data_files_all: {e}")
        return []

async def process_s3_files_async(from_date_str, to_date_str):
   
    s3_prefix = "digital/collectors/pagoefectivo/liquidations/"
    s3_files = list_files_in_s3(s3_prefix)
    
    dataframes = []
    s3_client = get_s3_client_with_role()
    
    columnas = [
        'SN1', 'SN2', 'CIP', 'IdComercio', 'SN3', 'Comisión', 'Total', 
        'Fecha Emisión', 'Fecha Cancelación', 'SN4', 'SN5', 'SN6', 
        'Banco', 'SN7', 'Canal'
    ]
    
    for s3_key in s3_files:
        if s3_key.endswith('.txt') and '/processed/' not in s3_key:
            try:
                content = read_file_from_s3(s3_key)
                
                # Leer el archivo .txt como CSV
                df = pd.read_csv(
                    BytesIO(content), 
                    sep=',', 
                    header=None, 
                    names=columnas,
                    dtype={'CIP': str, 'IdComercio': str}
                )
                
                # Extraer referencia del nombre del archivo
                match = re.search(r'(\d+)_', s3_key)
                referencia = match.group(1) if match else "SIN_REF"
                df["Referencia"] = referencia
                
                # Convertir y filtrar por fecha
                df['Fecha Cancelación'] = pd.to_datetime(
                    df['Fecha Cancelación'], 
                    format='%d/%m/%Y %H:%M:%S', 
                    errors='coerce'
                )
                from_date_dt = pd.to_datetime(from_date_str, format='%d.%m.%Y %H:%M:%S')
                to_date_dt = pd.to_datetime(to_date_str, format='%d.%m.%Y %H:%M:%S')
                
                df = df[
                    (df['Fecha Cancelación'] >= from_date_dt) & 
                    (df['Fecha Cancelación'] <= to_date_dt)
                ]
                
                dataframes.append(df)
                
                # Mover a processed
                if '/processed/' not in s3_key:
                    new_key = s3_key.replace('/liquidations/', '/liquidations/processed/', 1)
                    s3_client.copy_object(
                        Bucket=Config.S3_BUCKET,
                        CopySource={'Bucket': Config.S3_BUCKET, 'Key': s3_key},
                        Key=new_key
                    )
                    delete_file_from_s3(s3_key)
                
            except Exception as e:
                print(f"[ERROR] Error al procesar {s3_key}: {e}")

    return dataframes

async def get_data_join_async(from_date, to_date):
    # Obtener token del cache
    bearer = await token_cache_pagoefectivo.get_token()
    if not bearer:
        print("[ERROR] No se pudo obtener token de PagoEfectivo")
        return False
    
    start_date = from_date.replace(hour=0, minute=0, second=0)
    end_date = to_date.replace(hour=23, minute=59, second=59)
    
    from_date_str = start_date.strftime("%d.%m.%Y %H:%M:%S") 
    to_date_str = end_date.strftime("%d.%m.%Y %H:%M:%S")
    
    print(f"[DEBUG] Fechas a filtrar desde {from_date_str} hasta {to_date_str} en archivo")
    
    try:
        # Descargar archivos
        downloaded_files = await get_data_files_all_async(bearer, from_date, to_date)
        
        if downloaded_files:
            # Procesar archivos S3
            dataframes = await process_s3_files_async(from_date_str, to_date_str)
            
            if dataframes:
                consolidated_df = pd.concat(dataframes, ignore_index=True)
                
                # Convertir columnas numericas
                consolidated_df['Comisión'] = pd.to_numeric(consolidated_df['Comisión'], errors='coerce')
                consolidated_df['Total'] = pd.to_numeric(consolidated_df['Total'], errors='coerce')
                
                # Calcular Total Neto
                consolidated_df['Total Neto'] = consolidated_df['Total'] - consolidated_df['Comisión']
                
                # Seleccionar solo las columnas necesarias
                columnas_finales = [
                    'CIP', 'IdComercio', 'Comisión', 'Total', 'Total Neto', 
                    'Fecha Emisión', 'Fecha Cancelación', 'Banco', 'Canal', 'Referencia'
                ]
                consolidated_df = consolidated_df[columnas_finales]
                
                # Guardar en S3
                current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
                output_key = f"digital/collectors/pagoefectivo/liquidations/Pagoefectivo_Liquidaciones_{current_time}.csv"

                with BytesIO() as buffer:
                    consolidated_df.to_csv(buffer, index=False)
                    buffer.seek(0)
                    upload_file_to_s3(buffer.getvalue(), output_key)
                
                print(f"[INFO] Archivo consolidado guardado: {output_key}")
                
            else:
                print("[ALERTA] No se encontraron archivos TXT para consolidar.")
                return False
        else:
            print("[ALERTA] Sin datos descargados") 
            return False
        
    except Exception as e:
        print(f"[ERROR] Error en la ejecucion de get_data_join_PE: {e}")

# =============================
#   WRAPPER SINCRONO
# =============================

def get_data_join(from_date, to_date):
    print(f"[WRAPPER] Ejecutando PagoEfectivo collector Liquidations")
    return asyncio.run(get_data_join_async(from_date, to_date))
        
        
def get_data_pagoefectivo(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        # get_main_pagoefectivo(from_date, to_date)
        get_main_pagoefectivo(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de pagoefectivo: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/pagoefectivo/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, header=10, dtype={'CIP': str, 'Nro.Ord.Comercio': str, 'Nro Documento': str, 'Cliente Telefono': str})
                            
                        # Agregar al listado de DataFrames
                        dataframes.append(df)
                    
                    # Mover a processed
                    if '/input/' in s3_key and '/input/processed/' not in s3_key:
                        new_key = s3_key.replace('/input/', '/input/processed/', 1)
                        s3_client.copy_object(
                            Bucket=Config.S3_BUCKET,
                            CopySource={'Bucket': Config.S3_BUCKET, 'Key': s3_key},
                            Key=new_key
                        )
                        delete_file_from_s3(s3_key)
                    
                except Exception as e:
                    print(f"[✖] Error al procesar {s3_key}: {e}")
                    
            elif s3_key.endswith('.csv') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as csv_data:
                        df = pd.read_csv(csv_data, dtype={'CIP': str, 'Nro.Ord.Comercio': str, 'Nro Documento': str, 'Cliente Telefono': str})
                        if 'Nro.Ord.Comercio' in df.columns:
                            df['Nro.Ord.Comercio'] = (
                                df['Nro.Ord.Comercio']
                                .str.replace('ATP-', '', regex=False)
                                .str.replace('-ATP', '', regex=False)
                                .str.replace('-', '.', regex=False)
                                .astype(str)
                            )
                            
                        # Agregar al listado de DataFrames
                        dataframes.append(df)
                    
                    # Mover a processed
                    if '/input/' in s3_key and '/input/processed/' not in s3_key:
                        new_key = s3_key.replace('/input/', '/input/processed/', 1)
                        s3_client.copy_object(
                            Bucket=Config.S3_BUCKET,
                            CopySource={'Bucket': Config.S3_BUCKET, 'Key': s3_key},
                            Key=new_key
                        )
                        delete_file_from_s3(s3_key)
                    
                except Exception as e:
                    print(f"[✖] Error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)
            
            colums = ['CIP','Nro.Ord.Comercio','Monto','Estado','Fec.Emisión','Fec.Cancelación','Servicio','Cliente Nombre','Cliente Apellidos','Cliente Email','Tipo Doc.','Nro Documento','Cliente Telefono','Canal']
            
            consolidated_df=consolidated_df[colums]
            
            ## Logica de reglas
            
            def redondear_arriba_5(valor, decimales=2):
                valor_preciso = round(valor, 5)
                tercer_decimal = int(round(valor_preciso * 1000, 5)) % 10
                
                if tercer_decimal == 5:
                    valor_preciso += 0.001
                
                resultado = round(valor_preciso, decimales)
                return resultado
            
            def calcular_debito(total):
                if 0 < total <= 20:
                    return round((1.5 * 1.18), 2)
                elif 20.01 <= total <= 39.9:
                    return round((1.55 * 1.18), 2)
                elif 39.91 <= total <= 95:
                    return round((1.60 * 1.18), 2)
                elif 95.01 <= total:
                    fee = redondear_arriba_5((total * 0.035), 2)
                    imp = redondear_arriba_5((fee * 0.18), 2)
                    result = redondear_arriba_5((fee + imp), 2)
                    return result
                else:
                    return 0.00
                
            def calcular_debito_qr(total):
                if total > 0:
                    fee = redondear_arriba_5((total * 0.0295), 2)
                    imp = redondear_arriba_5((fee * 0.18), 2)
                    result = redondear_arriba_5((fee + imp), 2)
                    return result
                else:
                    return 0.00
                
            consolidated_df["DEBITO"] = np.where(
                consolidated_df['Servicio'] == 'Apuesta Total',
                consolidated_df["Monto"].apply(calcular_debito),
                consolidated_df["Monto"].apply(calcular_debito_qr)
            )
            consolidated_df["TOTAL NETO"] = consolidated_df["Monto"] - consolidated_df["DEBITO"]
            consolidated_df_f = consolidated_df[consolidated_df['Estado'].isin(['Cancelada'])]
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/pagoefectivo/liquidations/Pagoefectivo_Aprobados_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df_f.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            #download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
            
            print(f"[SUCCESS] Pagoefectivo-liq-apr procesado exitosamente: {output_key}")
            return True
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Pagoefectivo: {e}")
        return False
        
        
   
async def proccess_ref_txt_async(referencias):
    # Obtener token del cache
    bearer = await token_cache_pagoefectivo.get_token()
    if not bearer:
        print("[ERROR] No se pudo obtener token de PagoEfectivo")
        return {}
    
    referencia_dfs = {}
    
    lima_tz = pytz.timezone("America/Lima")
    today = datetime.now(lima_tz).strftime("%Y-%m-%d")
    from_d = today
    to_d = today
    
    # Obtener rutas de archivos completados
    print("[INFO] Obteniendo rutas de archivos completados...")
    referencias_enteros = [int(ref) for ref in referencias]
    rutas, referenc, nombres = await get_routes_liq_async(bearer, referencias_enteros, from_d, to_d)
    
    # Descargar archivos en paralelo
    print("[INFO] Descargando archivos...")
    download_tasks = []
    for id_reporte, referen, nombre in zip(rutas, referenc, nombres):
        if id_reporte and nombre:
            print(f"[INFO] Programando descarga para referencia {referen}: {nombre}")
            task = get_download_files_async(bearer, id_reporte, referen, nombre)
            download_tasks.append(task)
    
    # Ejecutar todas las descargas en paralelo
    download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
    
    # Verificar resultados de descargas
    successful_downloads = 0
    for i, result in enumerate(download_results):
        if result and not isinstance(result, Exception):
            successful_downloads += 1
        else:
            print(f"[ALERTA] Falló descarga para referencia {referenc[i]}")
    
    print(f"[INFO] Descargas completadas: {successful_downloads}/{len(download_tasks)}")
    
    # Definir los nombres de las columnas para los archivos de referencia
    columnas_ref = [
        'SN1', 'SN2', 'CIP', 'IdComercio', 'SN3', 'Comisión', 'Total', 
        'Fecha Emisión', 'Fecha Cancelación', 'SN4', 'SN5', 'SN6', 
        'Banco', 'SN7', 'Canal'
    ]
    
    # Procesar cada referencia
    for ref in referencias:
        ref_prefix = f"digital/collectors/pagoefectivo/liquidations/{ref}_"
        print(f"[INFO] Buscando archivo en ruta: {ref_prefix}")
        ult = get_latest_file_from_s3(ref_prefix)
        if ult:
            try:
                ref_content = read_file_from_s3(ult)
                
                # Leer el archivo .txt como CSV
                df_ref = pd.read_csv(
                    BytesIO(ref_content), 
                    sep=',', 
                    header=None, 
                    names=columnas_ref,
                    dtype={'CIP': str, 'IdComercio': str}
                )
                
                # Convertir columnas numericas
                df_ref['Comisión'] = pd.to_numeric(df_ref['Comisión'], errors='coerce')
                df_ref['Total'] = pd.to_numeric(df_ref['Total'], errors='coerce')
                
                # Convertir fechas
                df_ref['Fecha Emisión'] = pd.to_datetime(
                    df_ref['Fecha Emisión'], 
                    format='%d/%m/%Y %H:%M:%S', 
                    errors='coerce'
                )
                df_ref['Fecha Cancelación'] = pd.to_datetime(
                    df_ref['Fecha Cancelación'], 
                    format='%d/%m/%Y %H:%M:%S', 
                    errors='coerce'
                )
                
                # Seleccionar solo las columnas necesarias
                columnas_finales = [
                    'CIP', 'IdComercio', 'Comisión', 'Total', 
                    'Fecha Emisión', 'Fecha Cancelación', 'Banco', 'Canal'
                ]
                df_ref = df_ref[columnas_finales]
                
                referencia_dfs[ref] = df_ref
                print(f"[INFO] Referencia {ref} cargada correctamente - {len(df_ref)} registros")
                
            except Exception as e:
                print(f"[ERROR] Error al procesar referencia {ref}: {e}")
        else:
            print(f"[ALERTA] No se encontro archivo para la referencia {ref}")
    
    print(f"[INFO] Proceso completado. {len(referencia_dfs)}/{len(referencias)} referencias procesadas")
    return referencia_dfs

def proccess_ref_txt(referencias):
    return asyncio.run(proccess_ref_txt_async(referencias))
