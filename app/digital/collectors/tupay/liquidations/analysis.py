from app.digital.collectors.tupay.liquidations.utils import get_data_join
from app.digital.collectors.tupay.liquidations.utils import get_data_tupay
from app.digital.collectors.tupay.liquidations.email_handler import *
from app.digital.collectors.tupay.liquidations.utils import session_cache_tupay
from app.digital.collectors.tupay.liquidations.utils import export_settlement_to_s3

import pandas as pd
import numpy as np
import pytz
from datetime import datetime
from io import BytesIO
import asyncio
import re

from app.common.s3_utils import *
from app.common.database import *
from app.common.database import get_dts_session


  
def get_tupay_liq(from_date, to_date):
    # traemos la data de la liquidacion
    try:
        print("[INFO] DESCARGANDO DATA LIQUIDACION")
        get_data_join(from_date, to_date)
    except Exception as e:
        print(f"Error en Traer la dada de liquidacion para Tupay")
        return False
        
    # traemos la data de aprobados  
    try:
        print("[INFO] DESCARGANDO DATA TUPAY")
        get_data_tupay(from_date, to_date)
    except Exception as e:
        print(f"Error en traer la data de tupay")
        return False
        
    try:

        s3_client = get_s3_client_with_role()
        
        tupay_prefix = "digital/collectors/tupay/liquidations/Tupay_Aprobados_"
        liquidations_prefix = "digital/collectors/tupay/liquidations/Tupay_Liquidaciones_"
        
        tupay_key = get_latest_file_from_s3(tupay_prefix)
        liquidations_key = get_latest_file_from_s3(liquidations_prefix)

        if not tupay_key or not liquidations_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False
        
        print(f"[INFO] Procesando archivo Tupay: {tupay_key}")
        print(f"[INFO] Procesando archivo de Liquidacion: {liquidations_key}")
        
        # Leer archivos directamente desde S3
        tupay_content = read_file_from_s3(tupay_key)
        liquidation_content = read_file_from_s3(liquidations_key)

        df1 = pd.read_excel(BytesIO(tupay_content), dtype={'Invoice': str})
        df2 = pd.read_excel(BytesIO(liquidation_content), dtype={'External Reference': str, 'Referencia': str})
        
        cols_recaudador = ['Creation Date','Reference','Invoice','User Amount (local)','Fee (local)','Country tax fee (Local)','Status','Client Name','DEBITO','TOTAL NETO']
        cols_liquidador = ['Date Creation Minute','Id','Status','External Reference','Amount','Fee','Local Tax','Settlement Amount','Referencia']
        
        df1=df1[cols_recaudador]
        df2=df2[cols_liquidador]
        
        
        df1['Data'] = "<==>"
        
        
        df_conciliation = pd.merge(
            df1,
            df2,
            left_on='Invoice',
            right_on='External Reference',
            how='outer',
            indicator=True
        )
        
        df_conciliation = df_conciliation.rename(columns={'_merge': 'RESULTADO CONCILIACION'})
        # Cambiar valores
        df_conciliation['RESULTADO CONCILIACION'] = df_conciliation['RESULTADO CONCILIACION'].cat.rename_categories({
            'left_only': 'SOLO TUPAY',
            'right_only': 'SOLO LIQUIDACION',
            'both': 'CONCILIACION'
        })
        
        cols_recaudador_ = ['Creation Date','Reference','Invoice','User Amount (local)','Fee (local)','Country tax fee (Local)','Status_x','Client Name','DEBITO','TOTAL NETO']
        cols_liquidador_ = ['Date Creation Minute','Id','Status_y','External Reference','Amount','Fee','Local Tax','Settlement Amount','Referencia']
        
        df_conciliation_m = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['CONCILIACION'])]
        df_conciliation_r = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO TUPAY'])]
        df_conciliation_r = df_conciliation_r[cols_recaudador_]
        df_conciliation_l = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO LIQUIDACION'])]
        df_conciliation_l = df_conciliation_l[cols_liquidador_]

        
        response = ''
        
        if (round(df_conciliation['TOTAL NETO'].sum(), 2)) == (round(df_conciliation['Settlement Amount'].sum(), 2)):
            response = 'CONCILIACION EXITOSA'
        else:
            response = 'NO CONICILIACION EN LIQUIDACION'
        
        metricas = {
            "total_registros_tupay": len(df1),
            "total_registros_liquidacion": len(df2),
            
            "total_credito_tupay": round(df_conciliation['User Amount (local)'].sum(), 2),
            "total_credito_liquidacion": round(df_conciliation['Amount'].sum(), 2),
            "total_debito_tupay": round(df_conciliation['DEBITO'].sum(), 2),
            "total_debito_liquidacion": round(df_conciliation['Fee'].sum(), 2) + round(df_conciliation['Local Tax'].sum(), 2),
            "total_neto_tupay": round(df_conciliation['TOTAL NETO'].sum(), 2),
            "total_neto_liq": round(df_conciliation['Settlement Amount'].sum(), 2),
            
            "nc_credito_tupay": round(df_conciliation_r['User Amount (local)'].sum(), 2),
            "nc_credito_liquidacion": round(df_conciliation_l['Amount'].sum(), 2),
            "nc_debito_tupay": round(df_conciliation_r['DEBITO'].sum(), 2),
            "nc_debito_liquidacion": round(df_conciliation_l['Fee'].sum(), 2) + round(df_conciliation_l['Local Tax'].sum(), 2),
            "nc_neto_tupay": round(df_conciliation_r['TOTAL NETO'].sum(), 2),
            "nc_neto_liq": round(df_conciliation_l['Settlement Amount'].sum(), 2),
            
            "referencias": df_conciliation_m["Referencia"].unique().tolist(),
            "resultado": response
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
            
        
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/tupay/liquidations/processed/Tupay_Liquidations_Processed_{current_time}.xlsx"
        
        ## Descargamos y preparamos las referencias para agregar como hojas
        referencias = metricas['referencias']
        bearer_token = asyncio.run(session_cache_tupay.get_session())
        
        # Diccionario para almacenar los DataFrames de referencias
        referencia_dfs = {}
        
        # Descargar y leer las referencias
        for ref in referencias:
            print(f"[INFO] Descargando referencia {ref}")
            export_settlement_to_s3(bearer_token, ref )
            
            ref_prefix = f"digital/collectors/tupay/liquidations/{ref}_"
            print(f"[INFO] Buscando archivo en ruta: {ref_prefix}")
            ult = get_latest_file_from_s3(ref_prefix)
            if ult:
                ref_content = read_file_from_s3(ult)
                df_ref = pd.read_excel(BytesIO(ref_content))
                referencia_dfs[ref] = df_ref
                print(f"[INFO] Referencia {ref} cargada correctamente")
            else:
                print(f"[ALERTA] No se encontro archivo para la referencia {ref}")
        
        # Crear el Excel con todas las hojas
        with BytesIO() as excel_buffer:
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                # Hojas principales de conciliacion
                df_conciliation_m.to_excel(writer, sheet_name='Liquidaciones Conciliadas', index=False)
                df_conciliation_r.to_excel(writer, sheet_name='No Conciliadas Recaudador', index=False)
                df_conciliation_l.to_excel(writer, sheet_name='No Conciliadas Liquidacion', index=False)
                
                # Agregar hojas de referencias
                for ref, df_ref in referencia_dfs.items():
                    # Limitar el nombre de la hoja a 31 caracteres (limite de Excel)
                    sheet_name = f"Ref_{ref}"[:31]
                    df_ref.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"[INFO] Agregada hoja para referencia: {ref}")
            
            excel_buffer.seek(0)
            upload_file_to_s3(excel_buffer.getvalue(), output_key)
            print(f"[INFO] Archivo Excel guardado en S3: {output_key}")

        # Limpiar archivos temporales de referencias
        for ref in referencias:
            ref_prefix = f"digital/collectors/tupay/liquidations/{ref}_"
            ult = get_latest_file_from_s3(ref_prefix)
            if ult:
                try:
                    delete_file_from_s3(ult)
                    print(f"[INFO] Archivo temporal eliminado: {ult}")
                except Exception as e:
                    print(f"[ALERTA] No se pudo eliminar archivo temporal {ult}: {e}")

        # Mover archivos originales a processed
        new_tupay_key = tupay_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': tupay_key},
            Key=new_tupay_key
        )
        delete_file_from_s3(tupay_key)
        
        new_liquidations_key = liquidations_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': liquidations_key},
            Key=new_liquidations_key
        )
        delete_file_from_s3(liquidations_key)

        # enviamos el correo
        print(f"[DEBUG] Enviando correo")
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_liquidation_email(output_key, metricas, period_email )
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  
        
        # insertamos en la base de datos
        # insertamos en la base de datos (Dual: Local + AWS)
        def save_conciliacion_logic(session):
            liquidation_id = insert_liquidations(
                9,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["total_neto_tupay"],
                metricas["total_neto_liq"],
                
                metricas["total_registros_tupay"],
                metricas["total_registros_liquidacion"],
                metricas["total_debito_tupay"],
                metricas["total_debito_liquidacion"],
                metricas["total_credito_tupay"],
                metricas["total_credito_liquidacion"],
                
                metricas["nc_credito_tupay"],
                metricas["nc_credito_liquidacion"],
                metricas["nc_debito_tupay"],
                metricas["nc_debito_liquidacion"],
                metricas["nc_neto_tupay"],
                metricas["nc_neto_liq"],
            )
            insert_liquidation_files(
                session, liquidation_id, 1, f"s3://{Config.S3_BUCKET}/{new_tupay_key}"
            )
            insert_liquidation_files(
                session, liquidation_id, 1, f"s3://{Config.S3_BUCKET}/{new_liquidations_key}"
            )
            insert_liquidation_files(
                session, liquidation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()

        run_on_dual_dts(save_conciliacion_logic)
        
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True
        
    except Exception as e:
        print(f"Error en la conciliacion de liquidaciones para tupay: {e}")
        return False


        
    



        
