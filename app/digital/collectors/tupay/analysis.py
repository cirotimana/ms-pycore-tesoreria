import pandas as pd
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from app.digital.collectors.tupay.utils import *
from app.digital.collectors.tupay.email_handler import *
from app.common.database import *
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *

def get_data_tupay(from_date, to_date):
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[warn] error ejecutando la descarga de tupay: {e}")
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
                    
                    if '/input/' in s3_key and '/input/processed/' not in s3_key:
                        new_key = s3_key.replace('/input/', '/input/processed/', 1)
                        if copy_file_in_s3(s3_key, new_key):
                            delete_file_from_s3(s3_key)
                    
                except Exception as e:
                    print(f"[error] error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/tupay/output/Tupay_Ventas_{current_time}.csv"
            
            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False, encoding='utf-8')
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
            
            print(f"[ok] tupay procesado exitosamente: {output_key}")
            return True 
      
        else:
            print("[error] no se encontraron archivos excel para consolidar")
            return False

    except Exception as e:
        print(f"[error] error procesando datos tupay: {e}")
        return False 
        
def get_data_calimaco(from_date, to_date):
    try:
        method = 'TUPAY_QR,TUPAY'
        collector = 'tupay'
        calimaco_key = get_main_data(from_date, to_date, method, collector)

        if not calimaco_key:
            print('[error] no se pudo obtener los datos de calimaco')
            return False

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
        delete_file_from_s3(calimaco_key)
        
        print(f"[ok] calimaco procesado exitosamente: {output_key}")
        return True

    except Exception as e:
        print(f"[error] error en get_data_calimaco: {e}")
        return False
        
        
def conciliation_data(from_date, to_date):
    try:
        calimaco_prefix = "digital/collectors/tupay/calimaco/output/Calimaco_Tupay_Ventas_"
        tupay_prefix = "digital/collectors/tupay/output/Tupay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        tupay_key = get_latest_file_from_s3(tupay_prefix)

        if not calimaco_key or not tupay_key:
            print("[warn] no se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Tupay: {tupay_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        tupay_content = read_file_from_s3(tupay_key)

        calimaco_usecols = ["ID", "Fecha", "Fecha de modificación", "Estado", "Usuario", "Cantidad", "ID externo", "Comentarios"]
        tupay_usecols = ["Creation Date", "Invoice", "Reference", "Client Name", "User Amount (local)", "Status"]
        
        df1 = pd.read_csv(BytesIO(calimaco_content), usecols=calimaco_usecols, encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(tupay_content), usecols=tupay_usecols, encoding='utf-8', low_memory=False, dtype={'Reference': str, 'Invoice': str, 'Bank Reference' : str, 'Client Document' : str})
        
        df2 = df2.rename(columns={'Creation Date':'FECHA'})
        df2['FECHA'] = (pd.to_datetime(df2['FECHA'], errors='coerce') - timedelta(hours=5)).dt.strftime("%d/%m/%Y %H:%M:%S").fillna("")
        df2 = df2.rename(columns={'Invoice':'ID CALIMACO'})
        df2 = df2.rename(columns={'Reference':'ID PROVEEDOR'})
        df2 = df2.rename(columns={'Client Name':'CLIENTE'})
        df2 = df2.rename(columns={'User Amount (local)':'MONTO'})
        df2 = df2.rename(columns={'Status':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        df2 = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])].drop_duplicates(subset=['ID CALIMACO'], keep='first')
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])

        # Iniciar insercion en base de datos en paralelo
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 9)  
            bulk_upsert_calimaco_records_optimized(session, df1, 9)  
            
        executor = ThreadPoolExecutor(max_workers=1)
        db_future = executor.submit(run_on_dual_dts, initial_save)
        

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

        ## todos los que no son aprobados en calimaco
        df_no_aprovated_calimaco = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        ## todos los aprobados en calimaco
        df_aprovated_calimaco = df1[df1['Estado'] == 'Válido']
        ## todos los aprobados en el recaudador
        df_aprovated_recaudador = df2[df2['ESTADO PROVEEDOR'].isin(['COMPLETED'])]

        # cambio de estado no aprobados en calimaco vs aprobados en el recaudador
        df_cambio_estado = pd.merge(
        df_no_aprovated_calimaco,
        df_aprovated_recaudador,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        # conciliados aprobados calimaco vs aprobados recaudador
        df_conciliados = pd.merge(
        df_aprovated_calimaco,
        df_aprovated_recaudador,
        left_on='ID',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False)

        # duplicados_tupay
        df_duplicados = df2[df2.duplicated(subset=["ID CALIMACO"], keep=False)]
                
        # registros aprobados en calimaco que NO hicieron match con recaudador        
        df_no_conciliados = pd.merge(
            df_aprovated_calimaco,
            df_aprovated_recaudador,
            left_on='ID',
            right_on='ID CALIMACO',
            how='outer',
            indicator=True
        )

        df_no_conciliados = df_no_conciliados.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        df_no_conciliados['Recaudador Aprobado'] = df_no_conciliados['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Tupay Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que estan solo en uno de los dos
        df_no_conciliados_filtrado = df_no_conciliados[df_no_conciliados['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Tupay Aprobado'])]
        
        df_nc_calimaco = df_no_conciliados_filtrado[df_no_conciliados_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        df_nc_calimaco = df_nc_calimaco[cols_calimaco]
        df_nc_tupay = df_no_conciliados_filtrado[df_no_conciliados_filtrado['Recaudador Aprobado'] == 'Tupay Aprobado']
        df_nc_tupay = df_nc_tupay[cols_tupay]

        # guardar resultado en s3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Tupay_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_conciliados.to_excel(writer, sheet_name="Operaciones Conciliadas", index=False)
                df_nc_calimaco.to_excel(writer, sheet_name="No Conciliados Calimaco", index=False)
                df_nc_tupay.to_excel(writer, sheet_name="No Conciliados Proveedor", index=False)
                df_duplicados.to_excel(writer, sheet_name="Operaciones Duplicadas", index=False)
                df_cambio_estado.to_excel(writer, sheet_name="Cambios de Estado", index=False)
                df_aprovated_recaudador.to_excel(writer, sheet_name="Proveedor Original", index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)


        metricas = {
            "total_calimaco": len(df1),
            "total_tupay": len(df2),
            "aprobados_calimaco": len(df_aprovated_calimaco),
            "aprobados_tupay": len(df_aprovated_recaudador),
            "recaudacion_calimaco": round(df_aprovated_calimaco["Cantidad"].sum(), 2),
            "recaudacion_tupay": round(df_aprovated_recaudador["MONTO"].sum(), 2),
            "conciliados_total": len(df_conciliados),
            "conciliados_monto_calimaco": round(df_conciliados["Cantidad"].sum(), 2),
            "conciliados_monto_tupay": round(df_conciliados["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(df_nc_calimaco),
            "no_conciliados_tupay": len(df_nc_tupay),
            "no_conciliados_monto_calimaco": round(df_nc_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_tupay": round(df_nc_tupay["MONTO"].sum(), 2)
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")
    

        # mover a procesados
        # Tupay
        new_tupay_key = tupay_key.replace('/output/', '/output/processed/', 1)
        if copy_file_in_s3(tupay_key, new_tupay_key):
            delete_file_from_s3(tupay_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        if copy_file_in_s3(calimaco_key, new_calimaco_key):
            delete_file_from_s3(calimaco_key)

        # enviamos el reporte
        print("[info] enviando correo con resultados...")       
        period_email = f"{from_date.strftime('%Y/%m/%d')} - {to_date.strftime('%Y/%m/%d')}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  


        # Insertar en la db de forma dual
        def final_save(session):
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

        run_on_dual_dts(final_save)
        
        # Asegurar que la insercion inicial termino
        db_future.result()
                
        print(f"[ok] conciliacion completada exitosamente: {output_key}")
        return True        
        
    except Exception as e:
        print(f"[error] error en conciliation_data para tupay: {e}")
        return False

def updated_data_tupay():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/tupay/calimaco/output/Calimaco_Tupay_Ventas_"
        tupay_prefix = "digital/collectors/tupay/output/Tupay_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        tupay_key = get_latest_file_from_s3(tupay_prefix)

        if not calimaco_key or not tupay_key:
            print("[warn] no se encontraron archivos para actualizar")
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
        df2['FECHA'] = (pd.to_datetime(df2['FECHA'], errors='coerce') - timedelta(hours=5)).dt.strftime("%d/%m/%Y %H:%M:%S").fillna("")
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
        
        # Iniciar actualizacion en base de datos en paralelo
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 9) 
            bulk_upsert_calimaco_records_optimized(session, df1, 9)  
            update_collector_timestamp(session, 9) 

        executor = ThreadPoolExecutor(max_workers=1)
        db_future = executor.submit(run_on_dual_dts, update_save)
        
        # Eliminar archivos procesados mientras la DB trabaja
        delete_file_from_s3(tupay_key)
        delete_file_from_s3(calimaco_key)
        
        # Esperar a que la DB termine
        db_future.result()
        
        print("[ok] proceso exitoso para la actualizacion de tupay")
        return True
  
    except Exception as e:
        print(f"[error] error en updated_data_tupay: {e}")
        return False


