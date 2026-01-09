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
from app.digital.collectors.calimaco.main import *
import pytz

async def get_data_nuvei(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        filename = await get_main_download(from_date, to_date)
        if filename:
            print(f"[INFO] Archivo descargado: {filename}")
        else:
            return
    except Exception as e:
        print(f"[ERROR] Error ejecutando la descarga de nuvei: {e}")
        return False
    
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
                        print(f"[DEBUG] Archivo {s3_key}: {len(df)} registros")
                        
                        if 'Client Unique ID' in df.columns:
                            df['Client Unique ID'] = (
                                df['Client Unique ID']
                                .str.replace('ATP-', '', regex=False)
                                .str.replace('-', '.', regex=False)
                                .astype(str)
                            )
                        
                        # Validar fechas del archivo
                        if 'Date' in df.columns:
                            df['Date_parsed'] = pd.to_datetime(df['Date'], errors='coerce')
                            date_range = f"{df['Date_parsed'].min()} a {df['Date_parsed'].max()}"
                            print(f"[DEBUG] Rango de fechas en {s3_key}: {date_range}")
                        
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
                    print(f"[ERROR] Error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)
            print(f"[DEBUG] Total registros consolidados Nuvei: {len(consolidated_df)}")
            print(f"[DEBUG] Estados únicos consolidados: {consolidated_df['Transaction Result'].value_counts().to_dict()}")
            
            
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/nuvei/output/Nuvei_Ventas_{current_time}.xlsx"

            with BytesIO() as buffer:
                consolidated_df.to_excel(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            ##download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
            print(f"[SUCCESS] Nuvei procesado exitosamente: {output_key}")
            return True
        else:
            print("[ERROR] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[ERROR] Error procesando datos Nuvei: {e}")
        return False
    

async def get_data_calimaco(from_date, to_date):
    try:

        method = 'NUVEI'
        collector = 'nuvei'
        calimaco_key = await get_main_data_async(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Nuvei_Ventas_{current_time}.xlsx"
        
        with BytesIO() as buffer:
            df.to_excel(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
            
        delete_file_from_s3(calimaco_key)
        
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True 
    except Exception as e:
        print(f"[ERROR] Error en get_data_calimaco: {e}")
        return False
        

def conciliation_data(from_date, to_date):
    try:
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/nuvei/calimaco/output/Calimaco_Nuvei_Ventas_"
        nuvei_prefix = "digital/collectors/nuvei/output/Nuvei_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        nuvei_key = get_latest_file_from_s3(nuvei_prefix)

        if not calimaco_key or not nuvei_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False
        
        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Nuvei: {nuvei_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        nuvei_content = read_file_from_s3(nuvei_key)

        df1 = pd.read_excel(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_excel(BytesIO(nuvei_content), dtype={'Client Unique ID': str})
        
        df2 = df2.rename(columns={'Date':'FECHA'})
        df2 = df2.rename(columns={'Client Unique ID':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2['CLIENTE'] = '-'
        df2 = df2.rename(columns={'Amount':'MONTO'})
        df2 = df2.rename(columns={'Transaction Result':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        # Logs despues del filtro
        print(f"[DEBUG] Registros despues de filtros: {len(df2)}")
        approved_only = df2[df2['ESTADO PROVEEDOR'].isin(['Approved'])]
        print(f"[DEBUG] Solo Approved: {len(approved_only)}")
        
        df2 = approved_only.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        print(f"[DEBUG] Despues de eliminar duplicados: {len(df2)}")
        print(f"[DEBUG] Registros válidos Nuvei despues de filtrar fechas: {len(df2.dropna(subset=['FECHA']))}")
        print(f"[DEBUG] Registros válidos Calimaco despues de filtrar fechas: {len(df1.dropna(subset=['Fecha']))}")
        
        # filtrar registros con fechas validas antes de insertar
        df2_valid = df2.dropna(subset=['FECHA'])
        df1_valid = df1.dropna(subset=['Fecha'])
        
        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            print(f"[DEBUG] Insertando registros de Nuvei y Calimaco")
            if len(df2_valid) > 0:
                bulk_upsert_collector_records_optimized(session, df2_valid, 6)  
            if len(df1_valid) > 0:
                bulk_upsert_calimaco_records_optimized(session, df1_valid, 6) 
                
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
        
        cols_nuvei = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]

    
        # Condicion 1 - cambios de estado
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['Approved'])]
        conciliacion_cond1 = pd.merge(
        df1_cond1,
        df2_cond1,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        # condicion 2 _ data conciliada
        df1_cond2 = df1[df1['Estado'] == 'Válido']
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin (['Approved'])]
        conciliacion_cond2 = pd.merge(
        df1_cond2,
        df2_cond2,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        #condicion 3 - duplicados en nuvei
        duplicados_df2 = df2[df2.duplicated(subset=["ID CALIMACO"], keep=False)]
        
        #condicion 4 - aprobados que no hicieron match
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_nuvei = df2[df2['ESTADO PROVEEDOR'] == 'Approved']
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_nuvei,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )
        
        # Condicion 5 _ originales
        df2_original = df2.copy()
        df2_original = df2_original[df2_original['ESTADO PROVEEDOR'].isin(['Approved'])]
        
        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Nuvei Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Nuvei Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_nuvei = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Nuvei Aprobado']
        no_conciliados_nuvei = no_conciliados_nuvei[cols_nuvei]


        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Nuvei_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                conciliacion_cond2.to_excel(writer, sheet_name='Operaciones Conciliadas', index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name='No Conciliados Calimaco', index=False)
                no_conciliados_nuvei.to_excel(writer, sheet_name='No Conciliados Proveedor', index=False)
                duplicados_df2.to_excel(writer, sheet_name='Operaciones Duplicadas', index=False)
                conciliacion_cond1.to_excel(writer, sheet_name='Cambios de Estado', index=False)
                df2_original.to_excel(writer, sheet_name='Proveedor Original', index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
            
        ##download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
    
        ##analisamos igualdades
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")
    
        
        metricas = {
            "total_calimaco": len(df1),
            "total_nuvei": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_nuvei": len(approvals_df_nuvei),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_nuvei": round(approvals_df_nuvei['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_nuvei": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_nuvei": len(no_conciliados_nuvei),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_nuvei": round(no_conciliados_nuvei["MONTO"].sum(), 2)
        }
        
        print("[INFO] Datos obtenidos")
        for k, v in metricas.items():
            print(f"- {k}: {v}")


        # Mover archivos y obtener las rutas finales
        # Nuvei
        new_nuvei_key = nuvei_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': nuvei_key},
            Key=new_nuvei_key
        )
        delete_file_from_s3(nuvei_key)

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
                6,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_nuvei"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_nuvei"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_nuvei"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_nuvei"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_nuvei_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()

        run_on_dual_dts(final_save)
                
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True      
        
    except Exception as e:
        print(f"[ERROR] Error en conciliation_data para nuvei: {e}")
        return False


def updated_data_nuvei():
    try:
        calimaco_prefix = "digital/collectors/nuvei/calimaco/output/Calimaco_Nuvei_Ventas_"
        nuvei_prefix = "digital/collectors/nuvei/output/Nuvei_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        nuvei_key = get_latest_file_from_s3(nuvei_prefix)

        if not calimaco_key or not nuvei_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False
        
        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Nuvei: {nuvei_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        nuvei_content = read_file_from_s3(nuvei_key)

        df1 = pd.read_excel(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_excel(BytesIO(nuvei_content), dtype={'Client Unique ID': str})
        
        df2 = df2.rename(columns={'Date':'FECHA'})
        df2 = df2.rename(columns={'Client Unique ID':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2['CLIENTE'] = '-'
        df2 = df2.rename(columns={'Amount':'MONTO'})
        df2 = df2.rename(columns={'Transaction Result':'ESTADO PROVEEDOR'})     

        ## eliminar duplicados
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])   
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            print(f"[DEBUG] Insertando registros de Nuvei y Calimaco (Update)")
            if len(df2) > 0:
                bulk_upsert_collector_records_optimized(session, df2, 6)  
            if len(df1) > 0:
                bulk_upsert_calimaco_records_optimized(session, df1, 6) 
            update_collector_timestamp(session, 6) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(nuvei_key)
        delete_file_from_s3(calimaco_key)
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_nuvei: {e}")
        return False
    

if __name__ == "__main__":
    asyncio.run(updated_data_nuvei())
