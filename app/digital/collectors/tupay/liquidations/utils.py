from app.digital.collectors.tupay.utils import *
import pandas as pd
import asyncio
import requests
from datetime import datetime, timedelta
import pytz
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *
import re
from decimal import Decimal, ROUND_HALF_UP


# =============================
#   FUNCIONES ASINCRONAS
# =============================

async def get_id_async(bearer_cookie, from_date, to_date): 
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": bearer_cookie
    }
    
    url = "https://merchants-api.tupayonline.com/v1/settlements"

    params = {
        "page": 0,
        "from": from_date,
        "to": to_date,
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=500, verify=False)
        if response.status_code == 200:
            data = response.json()
            registros = data.get("data", [])
            
            if not registros:
                print("[INFO] sin registros")
                return []

            ids = [item.get("id") for item in registros if "id" in item]
            
            print(f"[INFO] Descargados {len(ids)} registros con id")
           
            return ids 
        else:
            print(f"[✖] Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"[✖] Excepcion durante la llamada en tupay: {e}")
        return []

async def export_settlement_to_s3_async(bearer_cookie, id):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": bearer_cookie,
    }

    url = f"https://merchants-api.tupayonline.com/v1/settlements/{id}/report"
    print(f"[INFO] Exportando liquidacion con id: {id} ")
    
    params = { "gmtOffset":"-5" }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=500, verify=False)

        if response.status_code == 200:
            export_data = response.json()
            storage_url = export_data.get("url")

            if not storage_url:
                print(f"[✖] No se recibio url en la respuesta: {export_data}")
                return None

            file_resp = requests.get(storage_url, timeout=500)

            if file_resp.status_code == 200:
                bytes = file_resp.content
                with BytesIO(bytes) as buffer:
                    now = datetime.now().strftime("%d%m%Y%H%M%S")
                    output_key = f"digital/collectors/tupay/liquidations/{id}_{now}.xlsx"
                    upload_file_to_s3(buffer.getvalue(), output_key)
                return output_key
            else:
                print(f"[✖] Error al descargar archivo real: {file_resp.status_code}")
                return None
        else:
            print(f"[✖] Error al exportar {id}: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"[✖] Excepcion al exportar {id}: {e}")
        return None

async def get_data_liq_async(from_date, to_date): 
    # Obtener sesion del cache
    bearer_cookie = await session_cache_tupay.get_session()
    if not bearer_cookie:
        print("[ERROR] No se pudo obtener sesion de Tupay")
        return

    from_dt = from_date.replace(hour=0, minute=0, second=0)
    to_dt = (to_date + timedelta(days=7)).replace(hour=23, minute=59, second=59)
    
    from_date_str = from_dt.strftime("%Y-%m-%d %H:%M:%S") 
    to_date_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[INFO] descargando transacciones del: {from_date_str} al {to_date_str}")
    
    from_d = date_to_timestamp(from_date_str)
    to_d = date_to_timestamp(to_date_str)
    
    print(f"[INFO] descargando transacciones del: {from_d} al {to_d} en formato timestamp")
    
    ids = await get_id_async(bearer_cookie, from_d, to_d)
    print(f"las id: {ids}")
    
    # Exportar liquidaciones en paralelo
    export_tasks = []
    for settlement_id in ids:
        task = export_settlement_to_s3_async(bearer_cookie, settlement_id)
        export_tasks.append(task)
    
    # Ejecutar todas las exportaciones en paralelo
    results = await asyncio.gather(*export_tasks, return_exceptions=True)
    
    successful_exports = 0
    for i, result in enumerate(results):
        if result and not isinstance(result, Exception):
            successful_exports += 1
        else:
            print(f"[ALERTA] Fallo exportacion para ID {ids[i]}")
    
    print(f"[INFO] Exportaciones completadas: {successful_exports}/{len(ids)}")

async def get_data_join_async(from_date, to_date):
    start_date = from_date.replace(hour=0, minute=0, second=0)
    end_date = to_date.replace(hour=23, minute=59, second=59)
    
    from_date_str = start_date.strftime("%Y-%m-%d %H:%M") 
    to_date_str = end_date.strftime("%Y-%m-%d %H:%M")
    
    print(f"[DEBUG] Fechas a filtrar desde {from_date_str} hasta {to_date_str} en archivo")
    
    s3_client = get_s3_client_with_role()
    try:
        await get_data_liq_async(from_date, to_date)
    except Exception as e:
        print(f"[ERROR] Error ejecutando get_data_liq: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/tupay/liquidations/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, engine='calamine', dtype={'External Reference': str})
                        match = re.search(r'(\d+)_', s3_key)
                        referencia = match.group(1) if match else "SIN_REF"

                        df["Referencia"] = referencia
                        
                        df['Date Creation Minute'] = pd.to_datetime(df['Date Creation Minute'], format='%Y-%m-%d %H:%M', errors='coerce')
                        from_date_dt = pd.to_datetime(from_date_str, format='%Y-%m-%d %H:%M')
                        to_date_dt = pd.to_datetime(to_date_str, format='%Y-%m-%d %H:%M')
                        df = df[(df['Date Creation Minute'] >= from_date_dt) & (df['Date Creation Minute'] <= to_date_dt)]
                        
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

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/tupay/liquidations/Tupay_Liquidaciones_{current_time}.xlsx"

            with BytesIO() as buffer:
                consolidated_df.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            print(f"[SUCCESS] Archivo consolidado guardado: {output_key}")
            return True
        else:
            print("[ALERTA] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[ERROR] Error procesando datos Tupay: {e}")
        return False

# =============================
#   WRAPPERS SINCRONOS
# =============================


def get_data_join(from_date, to_date):
    print(f"[WRAPPER] Ejecutando Tupay collector liquidations")
    return asyncio.run(get_data_join_async(from_date, to_date))

# =============================
#   DATA TUPAY VENTA
# =============================

    
def get_data_tupay(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de tupay: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/tupay/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.csv') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    df = pd.read_csv(BytesIO(content), dtype={'Reference': str, 'Invoice': str, 'Bank Reference' : str, 'Client Document' : str, 'Amount (USD)': str })
                    df['Creation Date'] = pd.to_datetime(df['Creation Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    df['Last Change Date'] = pd.to_datetime(df['Last Change Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    df['Expiration Date'] = pd.to_datetime(df['Expiration Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    
                    
                    if 'User Amount (local)' in df.columns:
                            df['User Amount (local)'] = (
                                df['User Amount (local)']
                                .astype(str)
                                .str.replace(",", "", regex=False)
                            )
                    df['User Amount (local)'] = pd.to_numeric(df['User Amount (local)'], errors="coerce")
                    
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
            
            
            consolidated_df["DEBITO"] = consolidated_df["Fee (local)"] + consolidated_df["Country tax fee (Local)"]
            consolidated_df["TOTAL NETO"] = consolidated_df["User Amount (local)"] - consolidated_df["DEBITO"]
            consolidated_df_f = consolidated_df[consolidated_df['Status'].isin(['COMPLETED'])]
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/tupay/liquidations/Tupay_Aprobados_{current_time}.xlsx"
            
            with BytesIO() as buffer:
                consolidated_df_f.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            #download_file_from_s3_to_local(output_key)
            
            print(f"[SUCCESS] Tupay-liq-apr procesado exitosamente: {output_key}")
            return True
      
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Tupay: {e}")
        return False
       
       

def export_settlement_to_s3(bearer_cookie, id):
    return asyncio.run(export_settlement_to_s3_async(bearer_cookie, id))
