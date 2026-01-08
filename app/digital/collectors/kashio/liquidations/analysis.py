from app.digital.collectors.kashio.liquidations.utils import get_data_join
from app.digital.collectors.kashio.liquidations.utils import get_data_kashio
from app.digital.collectors.kashio.liquidations.email_handler import *

import pandas as pd
import pytz
from datetime import datetime, timedelta
from io import BytesIO

from app.common.s3_utils import *
#from app.digital.collectors.kashio.utils import *
#from app.digital.collectors.kashio.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
#from app.digital.collectors.calimaco.main import *


def get_kashio_liq(from_date , to_date):
    # traemos la data de la liquidacion
    try:
        print("[INFO] DESCARGANDO DATA LIQUIDACION")
        get_data_join(from_date , to_date)
    except Exception as e:
        print(f"Error en Traer la dada de liquidacion para Kashio")
        return False
        
    # traemos la data de aprobados  
    try:
        print("[INFO] DESCARGANDO DATA KASHIO")
        get_data_kashio(from_date , to_date)
    except Exception as e:
        print(f"Error en traer la data de kashio")
        return False
        
    try:

        s3_client = get_s3_client_with_role()
        
        kashio_prefix = "digital/collectors/kashio/liquidations/Kashio_Aprobados_"
        liquidations_prefix = "digital/collectors/kashio/liquidations/Kashio_Liquidaciones_"
        
        kashio_key = get_latest_file_from_s3(kashio_prefix)
        liquidations_key = get_latest_file_from_s3(liquidations_prefix)

        if not kashio_key or not liquidations_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False
        
        print(f"[INFO] Procesando archivo Kashio: {kashio_key}")
        print(f"[INFO] Procesando archivo de Liquidacion: {liquidations_key}")
        
        # Leer archivos directamente desde S3
        kashio_content = read_file_from_s3(kashio_key)
        liquidation_content = read_file_from_s3(liquidations_key)

        df1 = pd.read_excel(BytesIO(kashio_content), dtype={'REFERENCIA DE ORDEN': str})
        df2 = pd.read_excel(BytesIO(liquidation_content), dtype={'REFERENCIA DE ORDEN': str})
        
        df1['Data'] = "<==>"
        
        df1 = df1.rename(columns={'REFERENCIA DE ORDEN':'REFERENCIA DE ORDEN KS'})
        df2 = df2.rename(columns={'REFERENCIA DE ORDEN':'REFERENCIA DE ORDEN LQ'})
        
        df_conciliation = pd.merge(
            df1,
            df2,
            left_on='REFERENCIA DE ORDEN KS',
            right_on='REFERENCIA DE ORDEN LQ',
            how='outer',
            indicator=True
        )
        
        df_conciliation = df_conciliation.rename(columns={'_merge': 'RESULTADO CONCILIACION'})
        # Cambiar valores
        df_conciliation['RESULTADO CONCILIACION'] = df_conciliation['RESULTADO CONCILIACION'].cat.rename_categories({
            'left_only': 'SOLO KASHIO',
            'right_only': 'SOLO LIQUIDACION',
            'both': 'CONCILIACION'
        })
        
        cols_recaudador = ['FECHA DE REGISTRO','REFERENCIA DE PAGO_x','CLIENTE_x','REFERENCIA DE ORDEN KS','DESCRIPCION','SUBTOTAL','MORA','TOTAL','METODO DE PAGO','OPERACION','ESTADO','DEBITO','TOTAL NETO']
        cols_liquidador = ['FECHA DE PAGO','REFERENCIA DE ORDEN LQ','CLIENTE_y','DESCRIPCIÓN','REFERENCIA DE PAGO_y','MONEDA','CRÉDITO','REFERENCIA DE COMISIÓN','MONEDA.1','DÉBITO','REFERENCIA','NETO']
        
        df_conciliation_m = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['CONCILIACION'])]
        df_conciliation_r = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO KASHIO'])]
        df_conciliation_r = df_conciliation_r[cols_recaudador]
        df_conciliation_l = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO LIQUIDACION'])]
        df_conciliation_l = df_conciliation_l[cols_liquidador]
        
        response = ''
        
        if (round(df_conciliation['TOTAL NETO'].sum(), 2)) == (round(df_conciliation['NETO'].sum(), 2)):
            response = 'CONCILIACION EXITOSA'
        else:
            response = 'NO CONICILIACION EN LIQUIDACION'
        
        metricas = {
            "total_registros_kashio": len(df1),
            "total_registros_liquidacion": len(df2),
            
            "total_credito_kashio": round(df_conciliation_m['TOTAL'].sum(), 2),
            "total_credito_liquidacion": round(df_conciliation_m['CRÉDITO'].sum(), 2),
            "total_debito_kashio": round(df_conciliation_m['DEBITO'].sum(), 2),
            "total_debito_liquidacion": round(df_conciliation_m['DÉBITO'].sum(), 2),
            "total_neto_kashio": round(df_conciliation_m['TOTAL NETO'].sum(), 2),
            "total_neto_liq": round(df_conciliation_m['NETO'].sum(), 2),
            
            
            "nc_credito_kashio": round(df_conciliation_r['TOTAL'].sum(), 2),
            "nc_credito_liquidacion": round(df_conciliation_l['CRÉDITO'].sum(), 2),
            "nc_debito_kashio": round(df_conciliation_r['DEBITO'].sum(), 2),
            "nc_debito_liquidacion": round(df_conciliation_l['DÉBITO'].sum(), 2),
            "nc_neto_kashio": round(df_conciliation_r['TOTAL NETO'].sum(), 2),
            "nc_neto_liq": round(df_conciliation_l['NETO'].sum(), 2),
            
            "referencias": df_conciliation["REFERENCIA"].unique().tolist(),
            "resultado": response
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
            
       
        
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/kashio/liquidations/processed/Kashio_Liquidations_Processed_{current_time}.xlsx"
        
        with BytesIO() as excel_buffer:
            #df_conciliation.to_excel(excel_buffer, index=False)
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_conciliation_m.to_excel(writer, sheet_name='Liquidaciones Conciliadas', index=False)
                df_conciliation_r.to_excel(writer, sheet_name='No Conciliadas Recaudador', index=False)
                df_conciliation_l.to_excel(writer, sheet_name='No Conciliadas Liquidacion', index=False)
            excel_buffer.seek(0)
            upload_file_to_s3(excel_buffer.getvalue(), output_key)

        #download_file_from_s3_to_local(output_key)
        
        ##movemos todo a procesado
        new_kashio_key = kashio_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': kashio_key},
            Key=new_kashio_key
        )
        delete_file_from_s3(kashio_key)
        
        new_liquidations_key = liquidations_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': liquidations_key},
            Key=new_liquidations_key
        )
        delete_file_from_s3(liquidations_key)

        # enviamos el correo
        print(f"[DEBUG] Enviando correo del perido:")
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_liquidation_email(output_key, metricas, period_email )
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  
        
        # insertamos en la base de datos
        with next(get_dts_session()) as session:
            liquidation_id = insert_liquidations(
                1,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["total_neto_kashio"],
                metricas["total_neto_liq"],
                
                metricas["total_registros_kashio"],
                metricas["total_registros_liquidacion"],
                metricas["total_debito_kashio"],
                metricas["total_debito_liquidacion"],
                metricas["total_credito_kashio"],
                metricas["total_credito_liquidacion"],
                
                metricas["nc_credito_kashio"],
                metricas["nc_credito_liquidacion"],
                metricas["nc_debito_kashio"],
                metricas["nc_debito_liquidacion"],
                metricas["nc_neto_kashio"],
                metricas["nc_neto_liq"],
                
                
            )
            insert_liquidation_files(
                session, liquidation_id, 1, f"s3://{Config.S3_BUCKET}/{new_kashio_key}"
            )
            insert_liquidation_files(
                session, liquidation_id, 1, f"s3://{Config.S3_BUCKET}/{new_liquidations_key}"
            )
            insert_liquidation_files(
                session, liquidation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()

        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True
        
    except Exception as e:
        print(f"Error en la conciliacion de liquidaciones para kashio {e}")
        return False
    
        
if __name__ == "__main__":
    get_kashio_liq()
        
    



        
