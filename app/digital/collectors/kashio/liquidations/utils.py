from app.digital.collectors.kashio.utils import get_data_main
from app.digital.collectors.kashio.utils import get_token_kashio, token_cache_kashio
import pandas as pd
import asyncio
import requests
from datetime import datetime, timedelta
import pytz
from app.config import Config
from io import BytesIO
from app.common.s3_utils import *
import re


async def get_public_id_async(token, from_date, to_date): 
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }
    url = "https://ns10-api-web-extranet.kashio.com.pe/kcms/v1/customers/cus_TZgE7VA6xSxTjmN8eutJcm/settlements"

    limit = 10
    offset = 0
    
    params = {
        "from_date": from_date,
        "to_date": to_date,
        "limit": limit,
        "start": offset,
        "status": "all"
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=500)
        if response.status_code == 200:
            data = response.json()
            registros = data.get("data", [])
            
            if not registros:
                print("[INFO] sin registros")
                return []

            public_ids = [item.get("public_id") for item in registros if "public_id" in item]
            reference = [item.get("reference") for item in registros if "reference" in item]

            print(f"[INFO] Descargados {len(public_ids)} registros con public_id")
            print(f"[INFO] Referencia del archivo: {reference}")
            
            return public_ids, reference
        else:
            print(f"[✖] Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"[✖] Excepcion durante la llamada en kashio: {e}")
        return []

async def export_settlement_to_s3_async(token, public_id, reference):
    headers = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json"
    }

    url = f"https://ns10-api-web-extranet.kashio.com.pe/kcms/v1/settlement_process/{public_id}/export"
    print(f"[INFO] Exportando liquidacion con public_id: {public_id} con la referencia {reference}")
    
    payload = { "template":"EXCEL_V2" }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=500)

        if response.status_code == 200:
            export_data = response.json()
            storage_url = export_data.get("storage_url")

            if not storage_url:
                print(f"[✖] No se recibio storage_url en la respuesta: {export_data}")
                return None

            file_resp = requests.get(storage_url, timeout=500)

            if file_resp.status_code == 200:
                excel_bytes = file_resp.content
                with BytesIO(excel_bytes) as excel_buffer:
                    now = datetime.now().strftime("%H%M%S")
                    output_key = f"digital/collectors/kashio/liquidations/{public_id}_{reference}_{now}.xlsx"
                    upload_file_to_s3(excel_buffer.getvalue(), output_key)
                return output_key
            else:
                print(f"[✖] Error al descargar archivo real: {file_resp.status_code}")
                return None
        else:
            print(f"[✖] Error al exportar {public_id}: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"[✖] Excepción al exportar {public_id}: {e}")
        return None

async def get_data_liq_async(from_date, to_date):
    try:
        start_date = from_date
        end_date = to_date + timedelta(days=2)
        
        print(f"[INFO] Descargando liquidaciones desde el {start_date} hasta el {end_date}")    
        
        # Obtener token del cache
        token = await token_cache_kashio.get_token()
        if not token:
            print("[ERROR] No se pudo obtener token de Kashio")
            return
            
        current_day = start_date
        while current_day < end_date:
            print(f"[INFO] Descargando liquidaciones por bloques para {current_day.date()}")
            from_dt = current_day.replace(hour=5, minute=0, second=0)
            to_dt = (current_day + timedelta(days=1)).replace(hour=5, minute=0, second=0)
            from_date_str = from_dt.strftime("%Y-%m-%d %H:%M:%S")
            to_date_str = to_dt.strftime("%Y-%m-%d %H:%M:%S")  
            
            ids, references = await get_public_id_async(token, from_date_str, to_date_str)
            print(f"IDs encontrados: {ids}")
            print(f"Referencias: {references}")
            
            # Exportar cada liquidación
            for settlement_id, reference in zip(ids, references):
                await export_settlement_to_s3_async(token, settlement_id, reference)
                
            current_day += timedelta(days=1)
    except Exception as e:
        print(f"Error en ejecutar get data {e}")

def get_data_liq(from_date, to_date):
    return asyncio.run(get_data_liq_async(from_date, to_date))


def get_data_join(from_date, to_date):
    start_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    from_date_str = start_date.strftime("%d/%m/%Y %H:%M:%S") 
    to_date_str = end_date.strftime("%d/%m/%Y %H:%M:%S")
    
    print(f"[DEBUG] Fechas a filtrar desde {from_date_str} hasta {to_date_str} en archivo")
    
    s3_client = get_s3_client_with_role()
    
    try:
        # Usar la versión asíncrona
        asyncio.run(get_data_liq_async(from_date, to_date))
    except Exception as e:
        print(f"[ERROR] Error ejecutando get_data_liq: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/kashio/liquidations/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, header=5, dtype={'REFERENCIA DE ORDEN': str})
                        df = df.drop(df.index[0])
                        
                        match = re.search(r'_(SE\d+)_', s3_key)
                        referencia = match.group(1) if match else "SIN_REF"
                        
                        df["REFERENCIA"] = referencia
                
                        if 'REFERENCIA DE ORDEN' in df.columns:
                            df['REFERENCIA DE ORDEN'] = (
                                df['REFERENCIA DE ORDEN']
                                .str.replace('-ATP', '', regex=False)
                                .str.replace('-', '.', regex=False)
                                .astype(str)
                            )
                            
                        df['FECHA DE PAGO'] = pd.to_datetime(df['FECHA DE PAGO'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
                        from_date_dt = pd.to_datetime(from_date_str, format='%d/%m/%Y %H:%M:%S')
                        to_date_dt = pd.to_datetime(to_date_str, format='%d/%m/%Y %H:%M:%S')
                        df = df[(df['FECHA DE PAGO'] >= from_date_dt) & (df['FECHA DE PAGO'] < to_date_dt)]
                        
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
            consolidated_df["NETO"] = consolidated_df["CRÉDITO"] - consolidated_df["DÉBITO"]
            
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/kashio/liquidations/Kashio_Liquidaciones_{current_time}.xlsx"

            with BytesIO() as buffer:
                consolidated_df.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
                
            print(f"[SUCCESS] Kashio-liq procesado exitosamente: {output_key}")
            return True
            
        else:
            print("[ALERTA] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[ERROR] Error procesando datos Kashio: {e}")
        return False



def get_data_kashio(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de kashio: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/kashio/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, dtype={'REFERENCIA DE ORDEN': str})
            
                        dataframes.append(df)

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
            
            def calcular_debito(total):
                if 20 <= total <= 64:
                    return round(1.8 + (1.8 * 0.18), 2)
                elif total > 64:
                    return round((total * 0.028) + ((total * 0.028) * 0.18), 2)
                else:
                    return 0.00
                
            consolidated_df["DEBITO"] = consolidated_df["TOTAL"].apply(calcular_debito)
            consolidated_df["TOTAL NETO"] = consolidated_df["TOTAL"] - consolidated_df["DEBITO"]
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/kashio/liquidations/Kashio_Aprobados_{current_time}.xlsx"
            
            with BytesIO() as buffer:
                consolidated_df.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            #download_file_from_s3_to_local(output_key)
            
            print(f"[SUCCESS] Kashio-liq-apr procesado exitosamente: {output_key}")
            return True

        else:
            print("[ALERTA] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Kashio: {e}")
        return False
        

