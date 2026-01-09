import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.safetypay.utils import *
from app.digital.collectors.safetypay.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *

def get_data_safetypay(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de safetypay: {e}")
        return False
    
    try:
        s3_prefix = "digital/collectors/safetypay/input/"
        s3_files = list_files_in_s3(s3_prefix)
        
        dataframes = []
        
        for s3_key in s3_files:
            if s3_key.endswith('.xlsx') and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(excel_data, dtype={'Id. de operación': str, 'Id. de transacción': str, 'Id. de ventas de comerciantes' : str, 'Número de pedido del comerciante' : str })
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
            output_key = f"digital/collectors/safetypay/output/Safetypay_Ventas_{current_time}.csv"
            
            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False, encoding='utf-8')
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            #download_file_from_s3_to_local(file_key)(output_key)
            
            print(f"[SUCCESS] Safetypay procesado exitosamente: {output_key}")
            return True 
      
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Safetypay: {e}")
        return False
        
def get_data_calimaco(from_date, to_date):
    try:
        method = 'SAFETYPAY'
        collector = 'safetypay'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Safetypay_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        #download_file_from_s3_to_local(file_key)(output_key)
        delete_file_from_s3(calimaco_key)
        
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True

    except Exception as e:
        print(f"[✖] Error en get_data_calimaco: {e}")
        return False
        
def conciliation_data(from_date, to_date):
    try:
        
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/safetypay/calimaco/output/Calimaco_Safetypay_Ventas_"
        safetypay_prefix = "digital/collectors/safetypay/output/Safetypay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        safetypay_key = get_latest_file_from_s3(safetypay_prefix)

        if not calimaco_key or not safetypay_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Safetypay: {safetypay_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        safetypay_content = read_file_from_s3(safetypay_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(safetypay_content), encoding='utf-8', low_memory=False, dtype={'Id. de ventas de comerciantes': str, 'Id. de operación' : str})
        
        df2 = df2.rename(columns={'Fecha':'FECHA'})
        df2 = df2.rename(columns={'Id. de ventas de comerciantes':'ID CALIMACO'})
        df2 = df2.rename(columns={'Id. de operación':'ID PROVEEDOR'})
        df2['CLIENTE'] = '-'
        df2 = df2.rename(columns={'Importe de la venta':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Compra completada'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])

        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 8)  # 8 = Safetypay
            bulk_upsert_calimaco_records_optimized(session, df1, 8)  # 8 = Safetypay
        
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
        
        cols_safetypay = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]

  
        # condicion 1 _ cambios de estado
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['Compra completada'])]
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
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin (['Compra completada'])]
        conciliacion_cond2 = pd.merge(
        df1_cond2,
        df2_cond2,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False
        )
        
        # condicion 3 _ duplicados
        df2_cond3 = df2[df2['ESTADO PROVEEDOR'].isin (['Compra completada'])]
        duplicados_df2 = df2_cond3[df2_cond3.duplicated(subset=["ID CALIMACO"], keep=False)]
       
        # condicion 4 _ aprovados sin match 
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_safetypay = df2[df2['ESTADO PROVEEDOR'].isin(['Compra completada'])]
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_safetypay,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )
        
        # condicion 5 _ original 
        df2_original = df2.copy()
        df2_original = df2_original[df2_original['ESTADO PROVEEDOR'].isin(['Compra completada'])]
        
        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Safetypay Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Safetypay Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_safetypay = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Safetypay Aprobado']
        no_conciliados_safetypay = no_conciliados_safetypay[cols_safetypay]

        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Safetypay_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                conciliacion_cond2.to_excel(writer, sheet_name='Operaciones Conciliadas', index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name='No Conciliados Calimaco', index=False)
                no_conciliados_safetypay.to_excel(writer, sheet_name='No Conciliados Proveedor', index=False)
                duplicados_df2.to_excel(writer, sheet_name='Operaciones Duplicadas', index=False)
                conciliacion_cond1.to_excel(writer, sheet_name='Cambios de Estado', index=False)
                df2_original.to_excel(writer, sheet_name='Proveedor Original', index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        #download_file_from_s3_to_local(file_key)(output_key)
        
        ##analisamos igualdades
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")
        

        metricas = {
            "total_calimaco": len(df1),
            "total_safetypay": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_safetypay": len(approvals_df_safetypay),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_safetypay": round(approvals_df_safetypay['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_safetypay": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_safetypay": len(no_conciliados_safetypay),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_safetypay": round(no_conciliados_safetypay["MONTO"].sum(), 2)

        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
       
        # Mover a procesados
        # Safetypay
        new_safetypay_key = safetypay_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': safetypay_key},
            Key=new_safetypay_key
        )
        delete_file_from_s3(safetypay_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': calimaco_key},
            Key=new_calimaco_key
        )
        delete_file_from_s3(calimaco_key)
        
        # Enviamos el correo
        print("[INFO] Enviando correo con resultados...")       
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # Insertar en base de datos de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                8,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_safetypay"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_safetypay"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_safetypay"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_safetypay"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_safetypay_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()       

        run_on_dual_dts(final_save)
                
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True
        
        
    except Exception as e:
        print(f"[✖] Error en conciliation_data para safetypay: {e}")
        return False
        
        
def updated_data_safetypay():
    try:       
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/safetypay/calimaco/output/Calimaco_Safetypay_Ventas_"
        safetypay_prefix = "digital/collectors/safetypay/output/Safetypay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        safetypay_key = get_latest_file_from_s3(safetypay_prefix)

        if not calimaco_key or not safetypay_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Safetypay: {safetypay_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        safetypay_content = read_file_from_s3(safetypay_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(safetypay_content), encoding='utf-8', low_memory=False, dtype={'Id. de ventas de comerciantes': str, 'Id. de operación' : str})
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'Fecha':'FECHA'})
        df2 = df2.rename(columns={'Id. de ventas de comerciantes':'ID CALIMACO'})
        df2 = df2.rename(columns={'Id. de operación':'ID PROVEEDOR'})
        df2['CLIENTE'] = '-'
        df2 = df2.rename(columns={'Importe de la venta':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # filtrar solo completadas
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Compra completada'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 8)  
            bulk_upsert_calimaco_records_optimized(session, df1, 8) 
            update_collector_timestamp(session, 8) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(safetypay_key)
        delete_file_from_s3(calimaco_key)
        
        print(f"[SUCCESS] Proceso exitoso para la actualizacion de safetypay")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_safetypay: {e}")
        return False
    
    
