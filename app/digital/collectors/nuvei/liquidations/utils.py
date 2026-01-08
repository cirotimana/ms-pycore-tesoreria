import asyncio
import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.nuvei.utils import *
from app.digital.collectors.nuvei.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.nuvei.utils import *
from io import BytesIO
from app.digital.collectors.calimaco.main_ import *


async def get_data_nuvei(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        filename = await get_main_download(from_date, to_date)
        if filename:
            print(f"[INFO] Archivo descargado para liquidaciones: {filename}")
        else:
            return
    except Exception as e:
        print(f"[ERROR] Error ejecutando la descarga de nuvei liquidaciones: {e}")
        return
    
    try:
        s3_prefix = "digital/collectors/nuvei/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, header=12, dtype={'Client Unique ID': str})
                        df = df.drop(df.index[-1])
                       
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
                    print(f"[ERROR] Error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/nuvei/liquidations/Nuvei_Aprobados_{current_time}.xlsx"

            with BytesIO() as buffer:
                consolidated_df.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            # download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
        else:
            print("[ERROR] No se encontraron archivos Excel para consolidar.")

    except Exception as e:
        print(f"[ERROR] Error procesando datos Nuvei: {e}")
        return
    


def is_valid_excel_improved(content):
    try:

        if len(content) < 100:
            print(f"[DEBUG] Contenido muy pequeño: {len(content)} bytes")
            return False
    
        if content.startswith(b'PK\x03\x04'):
            print("[DEBUG] Firma PK (ZIP/Excel) encontrada")
            return True
        
     
        if content.startswith(b'\xD0\xCF\x11\xE0'):  
            print("[DEBUG]Firma OLE2 (Excel antiguo) encontrada")
            return True
        
        
        # try:
        #     df = pd.read_excel(BytesIO(content), engine='openpyxl', nrows=1)
        #     print("[DEBUG] Pandas puede leer el archivo")
        #     return True
        # except:
        #     print("[DEBUG] Pandas no puede leer el archivo")
        #     return False
            
    except Exception as e:
        print(f"[DEBUG] Error en validacion: {e}")
        return False
    
    
def get_main_n(from_date=None, to_date=None, max_retries=10):
    lima_tz = pytz.timezone("America/Lima")
    now = datetime.now(lima_tz)
    if not from_date or not to_date:
        from_date = now - timedelta(days=7)
        to_date = from_date
    else:
        fmt = '%d%m%Y' if len(from_date) == 8 else '%d%m%y'
        from_date = datetime.strptime(from_date, fmt).replace(tzinfo=lima_tz)
        to_date = datetime.strptime(to_date, fmt).replace(tzinfo=lima_tz)

    print(f"[DEBUG] Enviando fechas from_date : {from_date} , to_date : {to_date}")

    try:
        session_data = None
        csrf_token = None
        session_cookies = None

        for retry in range(max_retries):
            session_data = asyncio.run(get_nuvei_session())

            csrf_token = session_data.get('csrf_token')
            session_cookies = session_data.get('cookies')

            # Mantener una sola sesion viva
            session = requests.Session()
            for k, v in session_cookies.items():
                session.cookies.set(k, v)

            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
                "Referer": "https://cpanel.nuvei.com/finance/settlement_summary"
            })

            success, status_code = get_settlement_summary(session, csrf_token, from_date, to_date)

            print(f"susses: {success}, status_code {status_code}")
            if status_code == 419:
                print("[WARN] Sesion expirada, obteniendo nueva sesion")
                session.close()
                if retry < max_retries - 1:
                    continue
                return None

            if not success:
                print("[ERROR] No se pudo enviar la solicitud")
                session.close()
                if retry < max_retries - 1:
                    time.sleep(5)
                    continue
                return None

            max_attempts = 20
            for attempt in range(1, max_attempts + 1):
                print(f"[INFO] Intento de descarga {attempt}/{max_attempts}")
                filename = try_download_excel_(session, attempt)

                if filename:
                    print(f"[OK] Archivo descargado: {filename}")
                    session.close()
                    return filename

                if attempt < max_attempts:
                    print(f"[INFO] Esperando 10 segundos antes del siguiente intento")
                    time.sleep(10)

            print(f"[ERROR] No se pudo descargar el archivo despues de {max_attempts} intentos")
            session.close()
            return None

    except Exception as e:
        print(f"Error en get_main: {e}")
        return False


def get_settlement_summary(session, csrf_token, from_date, to_date):
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-CSRF-TOKEN": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://cpanel.nuvei.com",
        "Referer": "https://cpanel.nuvei.com/finance/settlement_summary",
    }

    url = "https://cpanel.nuvei.com/report/finance/settlement-summary/run"

    date_from = from_date.strftime("%Y-%m-%d")
    date_to = to_date.strftime("%Y-%m-%d")

    print(f"fechas recibidas originales: {from_date} y {to_date}")
    print(f"convertidad: {from_date.strftime('%b %d, %Y')} - {to_date.strftime('%b %d, %Y')}")
    print(f"fechas recibidas : {date_from} y {date_to}")

    payload = {
        "_token": csrf_token,
        "pageEnd": "1",
        "page": "1",
        "pageSize": "25",
        "reportUrl": "report/finance/settlement-summary",
        "reportAbsoluteUrl": "https://cpanel.nuvei.com/report/finance/settlement-summary",
        "csrf_token": csrf_token,
        "is_export": "1",
        "is_import": "",
        "export_type": "excel",
        "action_performed": "0",
        "time_independent_filters": "",
        "reportSlug": "settlement-summary",
        "reportId": "119",
        "is_exporting_only": "0",
        "exporting_only_export_type": "pdf",
        "specialRunHandler": "0",
        "_drilldown": "0",
        "hasGraphQLDrillDown": "",
        "hasPermissionForActions": "",
        "settlementDateRangePicker": f"{from_date.strftime('%b %d, %Y')} - {to_date.strftime('%b %d, %Y')}",
        "acquirerBank": "-1",
        "settlementCurrency": "-1",
        "balanceView": "collapsed",
        "dateFrom": date_from,
        "dateTo": date_to,
    }

    columns = [
        "date",
        "id",
        "acquirerBank",
        "settlementCurrency",
        "clientName",
        "balanceDate",
        "settlementAmount",
        "transactionCurrency",
    ]

    for idx, col in enumerate(columns):
        payload[f"columnsOrder[{idx}]"] = col

    try:
        print(f"[INFO] Enviando solicitud de exportacion-liquidacion para {from_date.strftime('%d/%m/%Y')}")
        session.headers.update(headers)
        response = session.post(url, data=payload, timeout=60)

        if response.status_code == 200:
            print("[INFO] Solicitud de liquidacion enviada exitosamente")
            return True, None
        elif response.status_code == 419:
            print("[WARN] Sesion expirada (419)")
            return False, 419
        else:
            print(f"[ERROR] Status code {response.status_code}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            return False, response.status_code

    except Exception as e:
        print(f"[ERROR] Error al enviar solicitud: {e}")
        return False, None


def try_download_excel_(session, attempt):
    url = "https://cpanel.nuvei.com/exporter/export_download"

    try:
        response = session.get(url, timeout=60)

        if response.status_code != 200:
            print(f"[WARN] Intento {attempt}: Status {response.status_code}")
            return None

        content_type = response.headers.get("content-type", "")
        content_length = len(response.content)

        print(f"[DEBUG] Intento {attempt}: Content-Type: {content_type}")
        print(f"[DEBUG] Intento {attempt}: Content-Length: {content_length} bytes")
        print(f"[DEBUG] Intento {attempt}: Primeros bytes: {response.content[:500]}")

        if "text/html" in content_type:
            print(f"[WARN] Intento {attempt}: Archivo no listo aun (HTML)")
            return None

        if not is_valid_excel_improved(response.content):
            print(f"[WARN] Intento {attempt}: Contenido no es Excel valido")
            return None

        cd = response.headers.get("content-disposition", "")
        filename = None
        if cd:
            m = re.search(r'filename=([^;]+)', cd)
            if m:
                filename = m.group(1).strip('"')

        if not filename:
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            filename = f"nuvei_settlement_{current_time}.xlsx"

        print(f"[SUCCESS] Intento {attempt}: Excel valido encontrado - {filename} ({content_length} bytes)")

        s3_key = f"digital/collectors/nuvei/liquidations/{filename}"
        upload_file_to_s3(response.content, s3_key)
        # download_file_from_s3_to_local(s3_key)

        return filename

    except Exception as e:
        print(f"[ERROR] Intento {attempt}: {e}")
        return None


def get_ids_liquidations(from_date, to_date):
    try:
        filename = get_main_n(from_date, to_date)
        if filename:
            print(f"[INFO] Archivo descargado: {filename}")
        else:
            return
    except Exception as e:
        print(f"[ERROR] Error ejecutando la descarga de liquidaciones ids nuvei: {e}")
        return
    
    s3_key = f"digital/collectors/nuvei/liquidations/{filename}"
    
    print(f"S3_key: {s3_key}")
    
    content = read_file_from_s3(s3_key)
    with BytesIO(content) as excel_data:
        df = pd.read_excel(excel_data, header=11, dtype={'ID' : str})
        
        df.columns = df.columns.str.strip()
        print(df.columns.tolist())

        ids = (
            df["ID"]
            .dropna()
            .drop_duplicates()
            .astype(str)
            .tolist()
        )
        ids = [i for i in ids if i.strip().lower() != "nan" and i.strip() != ""]

    return ids


def get_settlement_summary_report(session, csrf_token, from_date, to_date):
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-CSRF-TOKEN": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://cpanel.nuvei.com",
        "Referer": "https://cpanel.nuvei.com/finance/settlement_summary",
    }

    url = "https://cpanel.nuvei.com/report/finance/settlement-summary/run"

    date_from = from_date.strftime("%Y-%m-%d")
    date_to = to_date.strftime("%Y-%m-%d")

    print(f"fechas recibidas originales: {from_date} y {to_date}")
    print(f"convertidad: {from_date.strftime('%b %d, %Y')} - {to_date.strftime('%b %d, %Y')}")
    print(f"fechas recibidas : {date_from} y {date_to}")

    payload = {
        "_token": csrf_token,
        "pageEnd": "1",
        "page": "1",
        "pageSize": "25",
        "reportUrl": "report/finance/settlement-summary",
        "reportAbsoluteUrl": "https://cpanel.nuvei.com/report/finance/settlement-summary",
        "csrf_token": csrf_token,
        "is_export": "1",
        "is_import": "",
        "export_type": "excel",
        "action_performed": "0",
        "time_independent_filters": "",
        "reportSlug": "settlement-summary",
        "reportId": "119",
        "is_exporting_only": "0",
        "exporting_only_export_type": "pdf",
        "specialRunHandler": "0",
        "_drilldown": "0",
        "hasGraphQLDrillDown": "",
        "hasPermissionForActions": "",
        "settlementDateRangePicker": f"{from_date.strftime('%b %d, %Y')} - {to_date.strftime('%b %d, %Y')}",
        "acquirerBank": "-1",
        "settlementCurrency": "-1",
        "balanceView": "collapsed",
        "dateFrom": date_from,
        "dateTo": date_to,
    }

    columns = [
        "date",
        "id",
        "acquirerBank",
        "settlementCurrency",
        "clientName",
        "balanceDate",
        "settlementAmount",
        "transactionCurrency",
    ]

    for idx, col in enumerate(columns):
        payload[f"columnsOrder[{idx}]"] = col

    try:
        print(f"[INFO] Enviando solicitud de exportacion-liquidacion para {from_date.strftime('%d/%m/%Y')}")
        session.headers.update(headers)
        response = session.post(url, data=payload, timeout=60)

        if response.status_code == 200:
            print("[INFO] Solicitud de liquidacion enviada exitosamente")
            return True, None
        elif response.status_code == 419:
            print("[WARN] Sesion expirada (419)")
            return False, 419
        else:
            print(f"[ERROR] Status code {response.status_code}")
            print(f"[DEBUG] Response: {response.text[:500]}")
            return False, response.status_code

    except Exception as e:
        print(f"[ERROR] Error al enviar solicitud: {e}")
        return False, None


if __name__ == "__main__":
    from_date = '27102025'
    to_date = '04112025'
    ids = get_ids_liquidations(from_date, to_date)
    
    print(ids)
