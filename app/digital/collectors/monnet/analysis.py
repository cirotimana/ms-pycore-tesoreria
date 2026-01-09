import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.monnet.utils import *
from app.digital.collectors.monnet.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *

def get_data_monnet(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de monnet: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/monnet/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data)
                        if 'Id Operacion Comercio' in df.columns:
                            df['Id Operacion Comercio'] = (
                                df['Id Operacion Comercio']
                                .str.replace('-', '.', regex=False)
                                .astype(str)
                            )
                        df = df[~df.isin(['Total']).any(axis=1)]
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

            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/monnet/output/Monnet_Ventas_{current_time}.csv"
            
            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False, encoding='utf-8')
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            print(f"[SUCCESS] Monnet procesado exitosamente: {output_key}")
            return True 
      
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Monnet: {e}")
        return False


def get_data_calimaco(from_date, to_date):
    try:

        method = 'MONNET,MONNET_QR'
        collector = 'monnet'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Monnet_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
            
        delete_file_from_s3(calimaco_key)
        
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True

    except Exception as e:
        print(f"[✖] Error en get_data_calimaco: {e}")
        return False


def conciliation_data(from_date, to_date):
    try:
        
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/monnet/calimaco/output/Calimaco_Monnet_Ventas_"
        monnet_prefix = "digital/collectors/monnet/output/Monnet_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        monnet_key = get_latest_file_from_s3(monnet_prefix)

        if not calimaco_key or not monnet_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Monnet: {monnet_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        monnet_content = read_file_from_s3(monnet_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(monnet_content), encoding='utf-8', low_memory=False, dtype={'Id Operacion Comercio': str})

        df2 = df2.rename(columns={'Fecha/hora de Registro':'FECHA'})
        df2 = df2.rename(columns={'Id Operacion Comercio':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'Nombre Cliente':'CLIENTE'})
        df2 = df2.rename(columns={'Monto':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Liquidado','Autorizado'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 2)  
            bulk_upsert_calimaco_records_optimized(session, df1, 2) 
        
        run_on_dual_dts(initial_save)
        

        cols_calimaco = [
            "ID",
            "Fecha de modificación",
            "Fecha",
            "Estado",
            "Usuario",
            "Cantidad",
            "ID externo",
            "Comentarios",
        ]
        
        cols_monnet = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]
                
        # condicion 1 _ cambios de estado
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['Liquidado','Autorizado'])]
        conciliacion_cond1 = pd.merge(
        df1_cond1,
        df2_cond1,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False
        )

        # condicion 2 _ conciliados
        df1_cond2 = df1[df1['Estado'] == 'Válido']
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin (['Liquidado','Autorizado'])]
        conciliacion_cond2 = pd.merge(
        df1_cond2,
        df2_cond2,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False
        )
        
        # condicion 3 _ duplicados
        df2_cond3 = df2[df2['ESTADO PROVEEDOR'].isin (['Liquidado','Autorizado'])]
        duplicados_df2 = df2_cond3[df2_cond3.duplicated(subset=["ID CALIMACO"], keep=False)]
       
        # condicion 4 _ aprovados sin match 
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_monnet = df2[df2['ESTADO PROVEEDOR'].isin(['Liquidado','Autorizado'])]
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_monnet,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )
        
        # condicion 5 _ original 
        df2_original = df2.copy()
        df2_original = df2_original[df2_original['ESTADO PROVEEDOR'].isin(['Liquidado','Autorizado'])]
        
        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Monnet Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Monnet Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_monnet = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Monnet Aprobado']
        no_conciliados_monnet = no_conciliados_monnet[cols_monnet]

        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Monnet_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                conciliacion_cond2.to_excel(writer, sheet_name='Operaciones Conciliadas', index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name='No Conciliados Calimaco', index=False)
                no_conciliados_monnet.to_excel(writer, sheet_name='No Conciliados Proveedor', index=False)
                duplicados_df2.to_excel(writer, sheet_name='Operaciones Duplicadas', index=False)
                conciliacion_cond1.to_excel(writer, sheet_name='Cambios de Estado', index=False)
                df2_original.to_excel(writer, sheet_name='Proveedor Original', index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        
        ##analisamos igualdades
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")

        metricas = {
            "total_calimaco": len(df1),
            "total_monnet": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_monnet": len(approvals_df_monnet),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_monnet": round(approvals_df_monnet['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_monnet": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_monnet": len(no_conciliados_monnet),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_monnet": round(no_conciliados_monnet["MONTO"].sum(), 2)

        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
         

        # Mover archivos y obtener las rutas finales
        # Monnet
        new_monnet_key = monnet_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': monnet_key},
            Key=new_monnet_key
        )
        delete_file_from_s3(monnet_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': calimaco_key},
            Key=new_calimaco_key
        )
        delete_file_from_s3(calimaco_key)
        
        # Enviar correo
        print("[INFO] Enviando correo con resultados")       
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # Insertar en la base de datos las rutas finales de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                2,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_monnet"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_monnet"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_monnet"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_monnet"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_monnet_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()     

        run_on_dual_dts(final_save)
                
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True 
    
    except Exception as e:
        print(f"[✖] Error en conciliation_data para monnet: {e}")
        return False
    
def updated_data_monnet():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/monnet/calimaco/output/Calimaco_Monnet_Ventas_"
        monnet_prefix = "digital/collectors/monnet/output/Monnet_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        monnet_key = get_latest_file_from_s3(monnet_prefix)

        if not calimaco_key or not monnet_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Monnet: {monnet_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        monnet_content = read_file_from_s3(monnet_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(monnet_content), encoding='utf-8', low_memory=False, dtype={'Id Operacion Comercio': str})
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'Fecha/hora de Registro':'FECHA'})
        df2 = df2.rename(columns={'Id Operacion Comercio':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'Nombre Cliente':'CLIENTE'})
        df2 = df2.rename(columns={'Monto':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # filtrar solo liquidados y autorizados
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Liquidado','Autorizado'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 2)  
            bulk_upsert_calimaco_records_optimized(session, df1, 2) 
            update_collector_timestamp(session, 2) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(monnet_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso de actualizacion para Monnet exitoso")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_monnet: {e}")
        return False
    
    
