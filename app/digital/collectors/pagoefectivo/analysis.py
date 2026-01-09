import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.pagoefectivo.utils import *
from app.digital.collectors.pagoefectivo.email_handler import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.common.database import *
from io import BytesIO
from app.digital.collectors.calimaco.main import *
from datetime import date
from datetime import date
import pytz


def get_data_pagoefectivo(from_date, to_date, method = 'CNC'):
    s3_client = get_s3_client_with_role()
    try:
        if method == 'UP':
            get_data_main_json( from_date, to_date)
        elif method == 'CNC':
            get_main_pagoefectivo(from_date, to_date)
        else:
            print(f"[ALERTA] Metodo desconocido: {method}")
            return False
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
            
            
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/pagoefectivo/output/PagoEfectivo_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            # download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
            print(f"[SUCCESS] PagoEfectivo procesado exitosamente: {output_key}")
            return True
        else:
            print("[✖] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Pagoefectivo: {e}")
        return False
    

def get_data_calimaco(from_date, to_date ):
    try:
        method = 'PAGOEFECTIVOQR,PAGOEFECTIVO'
        collector = 'pagoefectivo'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_PagoEfectivo_Ventas_{current_time}.csv"
        
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
    

def conciliation_data(from_date , to_date ):
    try:
        
        # archivos de donde se alimentaran los df
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/pagoefectivo/calimaco/output/Calimaco_PagoEfectivo_Ventas_"
        pagoefectivo_prefix = "digital/collectors/pagoefectivo/output/PagoEfectivo_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        pagoefectivo_key = get_latest_file_from_s3(pagoefectivo_prefix)

        if not calimaco_key or not pagoefectivo_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Pagoefectivo: {pagoefectivo_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        pagoefectivo_content = read_file_from_s3(pagoefectivo_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(pagoefectivo_content), encoding='utf-8', low_memory=False,  dtype={'CIP': str, 'Nro.Ord.Comercio': str, 'Nro Documento': str, 'Cliente Telefono': str})
        
        df2 = df2.rename(columns={'Fec.Cancelación':'FECHA'})
        df2 = df2.rename(columns={'Nro.Ord.Comercio':'ID CALIMACO'})
        df2 = df2.rename(columns={'CIP':'ID PROVEEDOR'})
        df2 = df2.rename(columns={'Cliente Nombre':'CLIENTE'})
        df2 = df2.rename(columns={'Monto':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Cancelada'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 7)  # 7 = PagoEfectivo
            bulk_upsert_calimaco_records_optimized(session, df1, 7)  # 7 = PagoEfectivo
        
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
        
        cols_pagoefectivo = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]


        # Condicion 1
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['Cancelada'])]
        conciliacion_cond1 = pd.merge(
        df1_cond1,
        df2_cond1,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        # Condicion 2
        df1_cond2 = df1[df1['Estado'] == 'Válido']
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin (['Cancelada'])]
        conciliacion_cond2 = pd.merge(
        df1_cond2,
        df2_cond2,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        # condicion 3 duplicados_pagoefectivo
        duplicados_df2 = df2[df2.duplicated(subset=["ID CALIMACO"], keep=False)]
                
        # condicion 4 registros aprobados en calimaco que NO hicieron match con pagoefectivo        
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_pagoefectivo = df2[df2['ESTADO PROVEEDOR'].isin(['Cancelada'])]
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_pagoefectivo,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )
        
        # condicion 5 _ originales
        df2_original = df2.copy()
        df2_original = df2_original[df2_original['ESTADO PROVEEDOR'].isin(['Cancelada'])]

        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'PagoEfectivo Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'PagoEfectivo Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_pagoefectivo = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'PagoEfectivo Aprobado']
        no_conciliados_pagoefectivo = no_conciliados_pagoefectivo[cols_pagoefectivo]

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/PagoEfectivo_Conciliacion_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            conciliacion_cond2.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        
        output_key_re = f"digital/apps/total-secure/conciliaciones/processed/PagoEfectivo_Conciliacion_Ventas_{current_time}.xlsx"
        
        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                save_dfs_to_excel(writer, {
                    "Operaciones Conciliadas": conciliacion_cond2,
                    "No Conciliados Calimaco": no_conciliados_calimaco,
                    "No Conciliados Proveedor": no_conciliados_pagoefectivo,
                    "Operaciones Duplicadas": duplicados_df2,
                    "Cambios de Estado": conciliacion_cond1,
                    "Proveedor Original": df2_original
                })
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key_re)
                    
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_csv(BytesIO(conciliacion_content), encoding='utf-8', low_memory=False)
            
        # download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
        

        metricas = {
            "total_calimaco": len(df1),
            "total_pagoefectivo": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_pagoefectivo": len(approvals_df_pagoefectivo),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_pagoefectivo": round(approvals_df_pagoefectivo['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_pagoefectivo": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_pagoefectivo": len(no_conciliados_pagoefectivo),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_pagoefectivo": round(no_conciliados_pagoefectivo["MONTO"].sum(), 2)
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
            
        # Mover archivos y obtener las rutas finales
        # Pagoefectivo
        new_pagoefectivo_key = pagoefectivo_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': pagoefectivo_key},
            Key=new_pagoefectivo_key
        )
        delete_file_from_s3(pagoefectivo_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': calimaco_key},
            Key=new_calimaco_key
        )
        delete_file_from_s3(calimaco_key)

        
        ## Enviamos el correo con los adjuntos
        print("[INFO] enviando correo con resultados")
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key_re, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # Insertar en la base de datos las rutas finales de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                7,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_pagoefectivo"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_pagoefectivo"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_pagoefectivo"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_pagoefectivo"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session,
                conciliation_id,
                1,
                f"s3://{Config.S3_BUCKET}/{new_pagoefectivo_key}",
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key_re}"
            )
            session.commit() 

        run_on_dual_dts(final_save)
        
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True
  
    except Exception as e:
        print(f"[✖] Error en conciliation_data para pagoefectivo: {e}")
        return False


def updated_data_pagoefectivo():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/pagoefectivo/calimaco/output/Calimaco_PagoEfectivo_Ventas_"
        pagoefectivo_prefix = "digital/collectors/pagoefectivo/output/PagoEfectivo_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        pagoefectivo_key = get_latest_file_from_s3(pagoefectivo_prefix)

        if not calimaco_key or not pagoefectivo_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Pagoefectivo: {pagoefectivo_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        pagoefectivo_content = read_file_from_s3(pagoefectivo_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(pagoefectivo_content), encoding='utf-8', low_memory=False,  dtype={'CIP': str, 'Nro.Ord.Comercio': str, 'Nro Documento': str, 'Cliente Telefono': str})
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'Fec.Cancelación':'FECHA'})
        df2 = df2.rename(columns={'Nro.Ord.Comercio':'ID CALIMACO'})
        df2 = df2.rename(columns={'CIP':'ID PROVEEDOR'})
        df2 = df2.rename(columns={'Cliente Nombre':'CLIENTE'})
        df2 = df2.rename(columns={'Monto':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['Cancelada'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 7)  
            bulk_upsert_calimaco_records_optimized(session, df1, 7)  
            update_collector_timestamp(session, 7) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(pagoefectivo_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso de actualizacion pagoefectivo exitoso")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_pagoefectivo: {e}")
        return False

