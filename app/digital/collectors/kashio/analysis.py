import pandas as pd
import pytz
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
from app.digital.collectors.kashio.utils import *
from app.digital.collectors.kashio.email_handler import *
from app.common.database import *
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *
import pytz


def get_data_kashio(from_date, to_date):
    # obtiene datos de kashio
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[warn] error ejecutando la descarga de kashio: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/kashio/input/"
        s3_files = list_files_in_s3(s3_prefix)

        dataframes = []

        for s3_key in s3_files:
            if s3_key.endswith(".xlsx") and "/input/processed/" not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                    with BytesIO(content) as excel_data:
                        df = pd.read_excel(
                            excel_data, dtype={"REFERENCIA DE ORDEN": str}
                        )

                        dataframes.append(df)

                    if "/input/" in s3_key and "/input/processed/" not in s3_key:
                        new_key = s3_key.replace("/input/", "/input/processed/", 1)
                        if copy_file_in_s3(s3_key, new_key):
                            delete_file_from_s3(s3_key)

                except Exception as e:
                    print(f"[error] error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)

            consolidated_df['ESTADO P'] = 'Aprobado'
            
            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
            output_key = f"digital/collectors/kashio/output/Kashio_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False, encoding="utf-8")
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)
                
            print(f"[ok] kashio procesado exitosamente: {output_key}")
            return True 

        else:
            print("[warn] no se encontraron archivos excel para consolidar")
            return False

    except Exception as e:
        print(f"[error] error procesando datos kashio: {e}")
        return False


def get_data_calimaco(from_date, to_date):
    # obtiene datos de calimaco
    try:
        method = "KASHIO"
        collector = "kashio"
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        
        if not calimaco_key:
            print("[error] no se obtuvo el archivo de calimaco")
            return False

        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content),encoding="utf-8",low_memory=False,dtype={"ID": str, "Usuario": str, "ID externo": str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True)  

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Kashio_Ventas_{current_time}.csv"

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
    # realiza la conciliacion
    try:
        calimaco_prefix = "digital/collectors/kashio/calimaco/output/Calimaco_Kashio_Ventas_"
        kashio_prefix = "digital/collectors/kashio/output/Kashio_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        kashio_key = get_latest_file_from_s3(kashio_prefix)

        if not calimaco_key or not kashio_key:
            print("[info] no se encontraron archivos para conciliar")
            return False

        print(f"[info] procesando archivo calimaco: {calimaco_key}")
        print(f"[info] procesando archivo kashio: {kashio_key}")

        # leer archivos directamente desde s3
        calimaco_content = read_file_from_s3(calimaco_key)
        kashio_content = read_file_from_s3(kashio_key)

        calimaco_usecols = ["ID", "Fecha", "Fecha de modificación", "Estado", "Usuario", "Cantidad", "ID externo", "Comentarios"]
        kashio_usecols = ["FECHA DE REGISTRO", "REFERENCIA DE ORDEN", "TOTAL", "ESTADO P", "CLIENTE"]

        df1 = pd.read_csv(BytesIO(calimaco_content),encoding="utf-8", low_memory=False, usecols=calimaco_usecols, dtype={"ID": str, "Usuario": str, "ID externo": str})
        df2 = pd.read_csv(BytesIO(kashio_content),encoding="utf-8", low_memory=False, usecols=kashio_usecols, dtype={"REFERENCIA DE ORDEN": str})
        
        
        df2 = df2.rename(columns={'FECHA DE REGISTRO':'FECHA'})
        df2 = df2.rename(columns={'REFERENCIA DE ORDEN':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'CLIENTE':'CLIENTE'})
        df2 = df2.rename(columns={'TOTAL':'MONTO'})
        df2 = df2.rename(columns={'ESTADO P':'ESTADO PROVEEDOR'})
        

        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1["Data"] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # Iniciar insercion en base de datos en paralelo
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 1)  
            bulk_upsert_calimaco_records_optimized(session, df1, 1)  
            
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
        
        cols_kashio = [
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
        df_aprovated_recaudador = df2.copy()

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

        # duplicados_kashio
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
            'right_only': 'Kashio Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que estan solo en uno de los dos
        df_no_conciliados_filtrado = df_no_conciliados[df_no_conciliados['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Kashio Aprobado'])]
        
        df_nc_calimaco = df_no_conciliados_filtrado[df_no_conciliados_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        df_nc_calimaco = df_nc_calimaco[cols_calimaco]
        df_nc_kashio = df_no_conciliados_filtrado[df_no_conciliados_filtrado['Recaudador Aprobado'] == 'Kashio Aprobado']
        df_nc_kashio = df_nc_kashio[cols_kashio]

        # guardar resultado en s3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Kashio_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df_conciliados.to_excel(writer, sheet_name="Operaciones Conciliadas", index=False)
                df_nc_calimaco.to_excel(writer, sheet_name="No Conciliados Calimaco", index=False)
                df_nc_kashio.to_excel(writer, sheet_name="No Conciliados Proveedor", index=False)
                df_duplicados.to_excel(writer, sheet_name="Operaciones Duplicadas", index=False)
                df_cambio_estado.to_excel(writer, sheet_name="Cambios de Estado", index=False)
                df_aprovated_recaudador.to_excel(writer, sheet_name="Proveedor Original", index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)

        metricas = {
            "total_calimaco": len(df1),
            "total_kashio": len(df2),
            "aprobados_calimaco": len(df_aprovated_calimaco),
            "aprobados_kashio": len(df_aprovated_recaudador),
            "recaudacion_calimaco": round(df_aprovated_calimaco["Cantidad"].sum(), 2),
            "recaudacion_kashio": round(df_aprovated_recaudador["MONTO"].sum(), 2),
            "conciliados_total": len(df_conciliados),
            "conciliados_monto_calimaco": round(df_conciliados["Cantidad"].sum(), 2),
            "conciliados_monto_kashio": round(df_conciliados["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(df_nc_calimaco),
            "no_conciliados_kashio": len(df_nc_kashio),
            "no_conciliados_monto_calimaco": round(df_nc_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_kashio": round(df_nc_kashio["MONTO"].sum(), 2)
        }

        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")

        new_kashio_key = kashio_key.replace("/output/", "/output/processed/", 1)
        if copy_file_in_s3(kashio_key, new_kashio_key):
            delete_file_from_s3(kashio_key)

        new_calimaco_key = calimaco_key.replace("/output/", "/output/processed/", 1)
        if copy_file_in_s3(calimaco_key, new_calimaco_key):
            delete_file_from_s3(calimaco_key)

        # enviar correo
        print("[info] enviando correo con resultados")
        period_email = f"{from_date.strftime('%Y/%m/%d')} - {to_date.strftime('%Y/%m/%d')}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # insertar en la base de datos las rutas finales de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                1,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_kashio"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_kashio"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_kashio"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_kashio"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_kashio_key}"
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
        print(f"[error] error en conciliation_data: {e}")
        return False
    
    
def updated_data_kashio():
    try:    
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/kashio/calimaco/output/Calimaco_Kashio_Ventas_"
        kashio_prefix = "digital/collectors/kashio/output/Kashio_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        kashio_key = get_latest_file_from_s3(kashio_prefix)

        if not calimaco_key or not kashio_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Kashio: {kashio_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        kashio_content = read_file_from_s3(kashio_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), encoding="utf-8", low_memory=False, dtype={"ID": str, "Usuario": str, "ID externo": str})
        df2 = pd.read_csv(BytesIO(kashio_content), encoding="utf-8", low_memory=False, dtype={"REFERENCIA DE ORDEN": str})
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'FECHA DE REGISTRO':'FECHA'})
        df2 = df2.rename(columns={'REFERENCIA DE ORDEN':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'CLIENTE':'CLIENTE'})
        df2 = df2.rename(columns={'TOTAL':'MONTO'})
        df2 = df2.rename(columns={'ESTADO P':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # eliminar duplicados
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # Iniciar actualizacion en base de datos en paralelo
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 1) 
            bulk_upsert_calimaco_records_optimized(session, df1, 1)  
            update_collector_timestamp(session, 1) 

        executor = ThreadPoolExecutor(max_workers=1)
        db_future = executor.submit(run_on_dual_dts, update_save)
        
        # Eliminar archivos procesados mientras la DB trabaja
        delete_file_from_s3(kashio_key)
        delete_file_from_s3(calimaco_key)
        
        # Esperar a que la DB termine
        db_future.result()
        
        print("[ok] proceso de actualizacion completado")
        return True
  
    except Exception as e:
        print(f"[error] error en updated_data_kashio: {e}")
        return False

