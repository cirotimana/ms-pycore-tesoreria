import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.tupay.utils import *
from app.digital.collectors.tupay.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *

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
                    df = pd.read_csv(BytesIO(content), dtype={'Reference': str, 'Invoice': str, 'Bank Reference' : str, 'Client Document' : str })
                    df['Creation Date'] = pd.to_datetime(df['Creation Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    df['Last Change Date'] = pd.to_datetime(df['Last Change Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    df['Expiration Date'] = pd.to_datetime(df['Expiration Date'], errors='coerce', dayfirst=True, format="mixed") - timedelta(hours=5)
                    
                    if 'Invoice' in df.columns:
                            df['Invoice'] = (
                                df['Invoice']
                                .str.replace('-ATP', '', regex=False)
                                .str.replace('-', '.', regex=False)
                                .astype(str)
                            )
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
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/tupay/output/Tupay_Ventas_{current_time}.csv"
            
            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False, encoding='utf-8')
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            ##download_file_from_s3_to_local(output_key)
            
            print(f"[SUCCESS] Tupay procesado exitosamente: {output_key}")
            return True 
      
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Tupay: {e}")
        return False 
        
def get_data_calimaco(from_date, to_date):
    try:
        method = 'TUPAY_QR,TUPAY'
        collector = 'tupay'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Tupay_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        ##download_file_from_s3_to_local(output_key)
        delete_file_from_s3(calimaco_key)
        
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True

    except Exception as e:
        print(f"[✖] Error en get_data_calimaco: {e}")
        return False
        
        
def conciliation_data(from_date, to_date):
    try:
        
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/tupay/calimaco/output/Calimaco_Tupay_Ventas_"
        tupay_prefix = "digital/collectors/tupay/output/Tupay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        tupay_key = get_latest_file_from_s3(tupay_prefix)

        if not calimaco_key or not tupay_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Tupay: {tupay_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        tupay_content = read_file_from_s3(tupay_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(tupay_content), encoding='utf-8', low_memory=False, dtype={'Reference': str, 'Invoice': str, 'Bank Reference' : str, 'Client Document' : str})
        
        df2 = df2.rename(columns={'Creation Date':'FECHA'})
        df2 = df2.rename(columns={'Invoice':'ID CALIMACO'})
        df2 = df2.rename(columns={'Reference':'ID PROVEEDOR'})
        df2 = df2.rename(columns={'Client Name':'CLIENTE'})
        df2 = df2.rename(columns={'User Amount (local)':'MONTO'})
        df2 = df2.rename(columns={'Status':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])

        # Insertar datos del collector (Tupay)
        with next(get_dts_session()) as session:
            bulk_upsert_collector_records_optimized(session, df2, 9)  

        # Insertar datos de Calimaco
        with next(get_dts_session()) as session:
            bulk_upsert_calimaco_records_optimized(session, df1, 9)  
        

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
        
        cols_tupay = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]

                
        # condicion 1 _ cambios de estado
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])]
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
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin (['COMPLETED'])]
        conciliacion_cond2 = pd.merge(
        df1_cond2,
        df2_cond2,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False
        )
        
        # condicion 3 _ duplicados
        df2_cond3 = df2[df2['ESTADO PROVEEDOR'].isin (['COMPLETED'])]
        duplicados_df2 = df2_cond3[df2_cond3.duplicated(subset=["ID CALIMACO"], keep=False)]
       
        # condicion 4 _ aprovados sin match 
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_tupay = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])]
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_tupay,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )
        
        # condicion 5 _ original 
        df2_original = df2.copy()
        df2_original = df2_original[df2_original['ESTADO PROVEEDOR'].isin(['COMPLETED'])]
        
        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Tupay Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Tupay Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_tupay = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Tupay Aprobado']
        no_conciliados_tupay = no_conciliados_tupay[cols_tupay]

        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Tupay_Conciliacion_Ventas_{current_time}.xlsx"
        

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                conciliacion_cond2.to_excel(writer, sheet_name='Operaciones Conciliadas', index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name='No Conciliados Calimaco', index=False)
                no_conciliados_tupay.to_excel(writer, sheet_name='No Conciliados Proveedor', index=False)
                duplicados_df2.to_excel(writer, sheet_name='Operaciones Duplicadas', index=False)
                conciliacion_cond1.to_excel(writer, sheet_name='Cambios de Estado', index=False)
                df2_original.to_excel(writer, sheet_name='Proveedor Original', index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        ##download_file_from_s3_to_local(output_key)
        
        ##analisamos igualdades
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")
        
        conciliadas_df["Cantidad"] = pd.to_numeric(conciliadas_df["Cantidad"], errors="coerce")
        conciliadas_df["MONTO"] = pd.to_numeric(conciliadas_df["MONTO"], errors="coerce")
        
    
        metricas = {
            "total_calimaco": len(df1),
            "total_tupay": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_tupay": len(approvals_df_tupay),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_tupay": round(approvals_df_tupay['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_tupay": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_tupay": len(no_conciliados_tupay),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_tupay": round(no_conciliados_tupay["MONTO"].sum(), 2)

        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
    

        # mover a procesados
        # Tupay
        new_tupay_key = tupay_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': tupay_key},
            Key=new_tupay_key
        )
        delete_file_from_s3(tupay_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': calimaco_key},
            Key=new_calimaco_key
        )
        delete_file_from_s3(calimaco_key)

        ## Enviamos el reporte
        print("[INFO] Enviando correo con resultados...")       
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  


        # Insertar en la db
        with next(get_dts_session()) as session:
                conciliation_id = insert_conciliations(
                    9,
                    session,
                    1,
                    from_date_fmt,
                    to_date_fmt,
                    metricas["recaudacion_calimaco"],
                    metricas["recaudacion_tupay"],
                    metricas["aprobados_calimaco"],
                    metricas["aprobados_tupay"],
                    metricas["no_conciliados_calimaco"],
                    metricas["no_conciliados_tupay"],
                    metricas["no_conciliados_monto_calimaco"],
                    metricas["no_conciliados_monto_tupay"],
                )
                insert_conciliation_files(
                    session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
                )
                insert_conciliation_files(
                    session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_tupay_key}"
                )
                insert_conciliation_files(
                    session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
                )
                session.commit() 
                
        print(f"[SUCCESS] Conciliación completada exitosamente: {output_key}")
        return True        
        
    except Exception as e:
        print(f"[✖] Error en conciliation_data para tupay: {e}")
        return False

def updated_data_tupay():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/tupay/calimaco/output/Calimaco_Tupay_Ventas_"
        tupay_prefix = "digital/collectors/tupay/output/Tupay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        tupay_key = get_latest_file_from_s3(tupay_prefix)

        if not calimaco_key or not tupay_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Tupay: {tupay_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        tupay_content = read_file_from_s3(tupay_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(tupay_content), encoding='utf-8', low_memory=False, dtype={'Reference': str, 'Invoice': str, 'Bank Reference' : str, 'Client Document' : str})
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'Creation Date':'FECHA'})
        df2 = df2.rename(columns={'Invoice':'ID CALIMACO'})
        df2 = df2.rename(columns={'Reference':'ID PROVEEDOR'})
        df2 = df2.rename(columns={'Client Name':'CLIENTE'})
        df2 = df2.rename(columns={'User Amount (local)':'MONTO'})
        df2 = df2.rename(columns={'Status':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # filtrar solo completados
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # insertar datos en base de datos
        with next(get_dts_session()) as session:
            bulk_upsert_collector_records_optimized(session, df2, 9)

        with next(get_dts_session()) as session:
            bulk_upsert_calimaco_records_optimized(session, df1, 9)  
            
        # actualizar timestamp del collector
        with next(get_dts_session()) as session:
            update_collector_timestamp(session, 9) 

        # eliminar archivos procesados
        delete_file_from_s3(tupay_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso exitoso para la actualizacion de Tupay")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_tupay: {e}")
        return False


