from app.digital.collectors.pagoefectivo.liquidations.utils import get_data_join
from app.digital.collectors.pagoefectivo.liquidations.utils import get_data_pagoefectivo
from app.digital.collectors.pagoefectivo.liquidations.email_handler import *
from app.digital.collectors.pagoefectivo.liquidations.utils import get_token_pagoefectivo
from app.digital.collectors.pagoefectivo.liquidations.utils import proccess_ref_txt
from app.digital.collectors.pagoefectivo.liquidations.utils import save_dfs_to_excel

import pandas as pd
import pytz
from datetime import datetime, timedelta
from io import BytesIO
import asyncio

from app.common.s3_utils import *
from app.common.database import *
from app.common.database import get_dts_session


  
def get_pagoefectivo_liq(from_date , to_date):
    ### traemos la data de la liquidacion
    try:
        print("[INFO] DESCARGANDO DATA LIQUIDACION")
        get_data_join(from_date, to_date)
    except Exception as e:
        print(f"Error en Traer la dada de liquidacion para Pagoefectivo")
        return False
        
    # traemos la data de aprobados  
    try:
        print("[INFO] DESCARGANDO DATA PAGOEFECTIVO")
        get_data_pagoefectivo(from_date, to_date)
    except Exception as e:
        print(f"Error en traer la data de pagoefectivo")
        return False
        
    try:

        s3_client = get_s3_client_with_role()
        
        pagoefectivo_prefix = "digital/collectors/pagoefectivo/liquidations/Pagoefectivo_Aprobados_"
        liquidations_prefix = "digital/collectors/pagoefectivo/liquidations/Pagoefectivo_Liquidaciones_"
        
        pagoefectivo_key = get_latest_file_from_s3(pagoefectivo_prefix)
        liquidations_key = get_latest_file_from_s3(liquidations_prefix)

        if not pagoefectivo_key or not liquidations_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False
        
        print(f"[INFO] Procesando archivo Pagoefectivo: {pagoefectivo_key}")
        print(f"[INFO] Procesando archivo de Liquidacion: {liquidations_key}")
        
        # Leer archivos directamente desde S3
        pagoefectivo_content = read_file_from_s3(pagoefectivo_key)
        liquidation_content = read_file_from_s3(liquidations_key)

        df1 = pd.read_csv(BytesIO(pagoefectivo_content), dtype={'Nro.Ord.Comercio': str, 'Nro Documento' : str})
        df2 = pd.read_csv(BytesIO(liquidation_content), dtype={'IdComercio': str, 'Referencia': str})
        
        cols_recaudador = ['CIP','Nro.Ord.Comercio','Estado','Fec.Emisión','Fec.Cancelación','Servicio','Cliente Nombre','Cliente Apellidos','Nro Documento','Canal', 'Monto', 'DEBITO', 'TOTAL NETO']
        cols_liquidador = ['CIP', 'IdComercio', 'Comisión', 'Total', 'Total Neto', 'Fecha Emisión', 'Fecha Cancelación', 'Banco', 'Canal', 'Referencia']
        
        df1=df1[cols_recaudador]
        df2=df2[cols_liquidador]
        
        
        df1['Data'] = "<==>"
        
        
        df_conciliation = pd.merge(
            df1,
            df2,
            left_on='Nro.Ord.Comercio',
            right_on='IdComercio',
            how='outer',
            indicator=True,
            suffixes=('_recaudador', '_liquidador') 
        )
        
        df_conciliation = df_conciliation.rename(columns={'_merge': 'RESULTADO CONCILIACION'})
        # Cambiar valores
        df_conciliation['RESULTADO CONCILIACION'] = df_conciliation['RESULTADO CONCILIACION'].cat.rename_categories({
            'left_only': 'SOLO PAGOEFECTIVO',
            'right_only': 'SOLO LIQUIDACION',
            'both': 'CONCILIACION'
        })
        
        cols_r = ['CIP_recaudador','Nro.Ord.Comercio','Estado','Fec.Emisión','Fec.Cancelación','Servicio','Cliente Nombre','Cliente Apellidos','Nro Documento','Canal_recaudador', 'Monto', 'DEBITO', 'TOTAL NETO']
        cols_l = ['CIP_liquidador','IdComercio','Fecha Emisión','Fecha Cancelación','Canal_liquidador','Total','Comisión', 'Total Neto', 'Referencia']
      
        
        df_conciliation_m = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['CONCILIACION'])]
        df_conciliation_r = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO PAGOEFECTIVO'])]
        df_conciliation_r = df_conciliation_r[cols_r]
        df_conciliation_l = df_conciliation[df_conciliation['RESULTADO CONCILIACION'].isin(['SOLO LIQUIDACION'])]
        df_conciliation_l = df_conciliation_l[cols_l]

        
        response = ''
        
        if (round(df_conciliation['TOTAL NETO'].sum(), 2)) == (round(df_conciliation['Total Neto'].sum(), 2)):
            response = 'CONCILIACION EXITOSA'
        else:
            response = 'NO CONICILIACION EN LIQUIDACION'
        
        metricas = {
            "total_registros_pagoefectivo": len(df1),
            "total_registros_liquidacion": len(df2),
            
            "total_credito_pagoefectivo": round(df_conciliation['Monto'].sum(), 2),
            "total_credito_liquidacion": round(df_conciliation['Total'].sum(), 2),
            "total_debito_pagoefectivo": round(df_conciliation['DEBITO'].sum(), 2),
            "total_debito_liquidacion": round(df_conciliation['Comisión'].sum(), 2),
            "total_neto_pagoefectivo": round(df_conciliation['TOTAL NETO'].sum(), 2),
            "total_neto_liq": round(df_conciliation['Total Neto'].sum(), 2),
            
            "nc_credito_pagoefectivo": round(df_conciliation_r['Monto'].sum(), 2),
            "nc_credito_liquidacion": round(df_conciliation_l['Total'].sum(), 2),
            "nc_debito_pagoefectivo": round(df_conciliation_r['DEBITO'].sum(), 2),
            "nc_debito_liquidacion": round(df_conciliation_l['Comisión'].sum(), 2),
            "nc_neto_pagoefectivo": round(df_conciliation_r['TOTAL NETO'].sum(), 2),
            "nc_neto_liq": round(df_conciliation_l['Total Neto'].sum(), 2),
            
            
            "referencias": df_conciliation_m["Referencia"].unique().tolist(),
            "resultado": response
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
            
        
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/pagoefectivo/liquidations/processed/Pagoefectivo_Liquidations_Processed_{current_time}.xlsx"
        
        # ## Descargamos y preparamos las referencias para agregar como hojas
        referencias = metricas['referencias']
        
        referencia_dfs = proccess_ref_txt(referencias)
        
        # Crear el Excel con todas las hojas
        with BytesIO() as excel_buffer:
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                save_dfs_to_excel(writer, {
                    "Liquidaciones Conciliadas": df_conciliation_m,
                    "No Conciliadas Recaudador": df_conciliation_r,
                    "No Conciliadas Liquidacion": df_conciliation_l
                })
                
                
                # Agregar hojas de referencias
                for ref, df_ref in referencia_dfs.items():
                    # Limitar el nombre de la hoja a 31 caracteres (limite de Excel)
                    sheet_name = f"Ref_{ref}"[:31]
                    df_ref.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"[INFO] Agregada hoja para referencia: {ref}")
            
            excel_buffer.seek(0)
            upload_file_to_s3(excel_buffer.getvalue(), output_key)
        ##download_file_from_s3_to_local(output_key)

        # Limpiar archivos temporales de referencias
        for ref in referencias:
            ref_prefix = f"digital/collectors/pagoefectivo/liquidations/{ref}_"
            ult = get_latest_file_from_s3(ref_prefix)
            if ult:
                try:
                    delete_file_from_s3(ult)
                    print(f"[INFO] Archivo temporal eliminado: {ult}")
                except Exception as e:
                    print(f"[ALERTA] No se pudo eliminar archivo temporal {ult}: {e}")

        # Mover archivos originales a processed
        new_pagoefectivo_key = pagoefectivo_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': pagoefectivo_key},
            Key=new_pagoefectivo_key
        )
        delete_file_from_s3(pagoefectivo_key)
        
        new_liquidations_key = liquidations_key.replace('/liquidations/', '/liquidations/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': liquidations_key},
            Key=new_liquidations_key
        )
        delete_file_from_s3(liquidations_key)

        # enviamos el correo
        print(f"[DEBUG] Enviando correo")
        from_str = from_date.strftime("%Y/%m/%d")
        to_str = to_date.strftime("%Y/%m/%d")
        period_email = f"{from_str} - {to_str}"
        send_liquidation_email(output_key, metricas, period_email )
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  
        
        # insertamos en la base de datos
        with next(get_dts_session()) as session:
            liquidation_id = insert_liquidations(
                7,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["total_neto_pagoefectivo"],
                metricas["total_neto_liq"],
                
                metricas["total_registros_pagoefectivo"],
                metricas["total_registros_liquidacion"],
                metricas["total_debito_pagoefectivo"],
                metricas["total_debito_liquidacion"],
                metricas["total_credito_pagoefectivo"],
                metricas["total_credito_liquidacion"],
                
                metricas["nc_credito_pagoefectivo"],
                metricas["nc_credito_liquidacion"],
                metricas["nc_debito_pagoefectivo"],
                metricas["nc_debito_liquidacion"],
                metricas["nc_neto_pagoefectivo"],
                metricas["nc_neto_liq"],
                
            )
            insert_liquidation_files(
                session, liquidation_id, 1, f"s3://{Config.S3_BUCKET}/{new_pagoefectivo_key}"
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
        print(f"Error en la conciliacion de liquidaciones para pagoefectivo: {e}")
        return False


        
    



        
