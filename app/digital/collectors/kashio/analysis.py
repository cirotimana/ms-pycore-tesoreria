import pandas as pd
import pytz
from datetime import datetime, date
from app.digital.collectors.kashio.utils import *
from app.digital.collectors.kashio.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *
import pytz

def get_data_kashio(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de kashio: {e}")
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

                    # Mover a processed
                    # delete_file_from_s3(s3_key)
                    if "/input/" in s3_key and "/input/processed/" not in s3_key:
                        new_key = s3_key.replace("/input/", "/input/processed/", 1)
                        s3_client.copy_object(
                            Bucket=Config.S3_BUCKET,
                            CopySource={"Bucket": Config.S3_BUCKET, "Key": s3_key},
                            Key=new_key,
                        )
                        delete_file_from_s3(s3_key)

                except Exception as e:
                    print(f"[✖] Error al procesar {s3_key}: {e}")

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
                
            print(f"[SUCCESS] Kashio procesado exitosamente: {output_key}")
            return True 

        else:
            print("[ALERTA] No se encontraron archivos Excel para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos Kashio: {e}")
        return False


def get_data_calimaco(from_date, to_date):
    try:
        method = "KASHIO"
        collector = "kashio"
        calimaco_key = get_main_data(from_date, to_date, method, collector)
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
        
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True

    except Exception as e:
        print(f"[✖] Error en get_data_calimaco: {e}")
        return False


def conciliation_data(from_date, to_date):
    try:

        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/kashio/calimaco/output/Calimaco_Kashio_Ventas_"
        kashio_prefix = "digital/collectors/kashio/output/Kashio_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        kashio_key = get_latest_file_from_s3(kashio_prefix)

        if not calimaco_key or not kashio_key:
            print("No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Kashio: {kashio_key}")

        # Leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        kashio_content = read_file_from_s3(kashio_key)

        df1 = pd.read_csv(BytesIO(calimaco_content),encoding="utf-8",low_memory=False,dtype={"ID": str, "Usuario": str, "ID externo": str},)
        df2 = pd.read_csv(BytesIO(kashio_content),encoding="utf-8",low_memory=False,dtype={"REFERENCIA DE ORDEN": str},)
        
        
        df2 = df2.rename(columns={'FECHA DE REGISTRO':'FECHA'})
        df2 = df2.rename(columns={'REFERENCIA DE ORDEN':'ID CALIMACO'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'CLIENTE':'CLIENTE'})
        df2 = df2.rename(columns={'TOTAL':'MONTO'})
        df2 = df2.rename(columns={'ESTADO P':'ESTADO PROVEEDOR'})
        

        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1["Data"] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # Insertar datos del collector (Kashio)
        with next(get_dts_session()) as session:
            bulk_upsert_collector_records_optimized(session, df2, 1)  

        # Insertar datos de Calimaco
        with next(get_dts_session()) as session:
            bulk_upsert_calimaco_records_optimized(session, df1, 1)  
      
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
        
        
        df2_original = df2.copy()

        ## condicion 1 - cambio de estado
        df1_cond1 = df1[df1["Estado"].isin(["Denegado", "Nuevo", "CANCELLED", "Límites excedidos"])]
        df2_cond1 = df2.copy()
        conciliacion_cond1 = pd.merge(
            df1_cond1,
            df2_cond1,
            left_on="ID",
            right_on="ID CALIMACO",
            how="inner",
            indicator=False,
        )
        ## condicion 2 - operaciones conciliadas
        df1_cond2 = df1[df1["Estado"] == "Válido"]
        df2_cond2 = df2.copy()
        conciliacion_cond2 = pd.merge(
            df1_cond2,
            df2_cond2,
            left_on="ID",
            right_on="ID CALIMACO",
            how="inner",
            indicator=False,
        )
        # condicion 3 - operaciones duplicadas en kashio
        duplicados_df2 = df2[df2.duplicated(subset=["ID CALIMACO"], keep=False)]

        # condicion 4.0 - operaciones aprobadas sin match
        approvals_df_calimaco = df1[df1["Estado"] == "Válido"]
        approvals_df_kashio = df2.copy()
        no_match = pd.merge(
            approvals_df_calimaco,
            approvals_df_kashio,
            left_on="ID",
            right_on="ID CALIMACO",
            how="outer",
            indicator=True,
        )

        no_match = no_match.rename(columns={"_merge": "Recaudador Aprobado"})
        # Cambiar valores
        no_match["Recaudador Aprobado"] = no_match["Recaudador Aprobado"].cat.rename_categories({
                "left_only": "Calimaco Aprobado",
                "right_only": "Kashio Aprobado",
                "both": "Ambos",
            }
        )
        # Filtrar solo los que estan solo en uno de los dos
        no_match_filtrado = no_match[no_match["Recaudador Aprobado"].isin(["Calimaco Aprobado", "Kashio Aprobado"])]

        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Kashio_Conciliacion_Ventas_{current_time}.xlsx"

        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado["Recaudador Aprobado"] == "Calimaco Aprobado"]
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_kashio = no_match_filtrado[no_match_filtrado["Recaudador Aprobado"] == "Kashio Aprobado"]
        no_conciliados_kashio = no_conciliados_kashio[cols_kashio]

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                conciliacion_cond2.to_excel(writer, sheet_name="Operaciones Conciliadas", index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name="No Conciliados Calimaco", index=False)
                no_conciliados_kashio.to_excel(writer, sheet_name="No Conciliados Proveedor", index=False)
                duplicados_df2.to_excel(writer, sheet_name="Operaciones Duplicadas", index=False)
                conciliacion_cond1.to_excel(writer, sheet_name="Cambios de Estado", index=False)
                df2_original.to_excel(writer, sheet_name="Proveedor Original", index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)

        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")


        metricas = {
            "total_calimaco": len(df1),
            "total_kashio": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_kashio": len(approvals_df_kashio),
            "recaudacion_calimaco": round(approvals_df_calimaco["Cantidad"].sum(), 2),
            "recaudacion_kashio": round(approvals_df_kashio["MONTO"].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_kashio": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_kashio": len(no_conciliados_kashio),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_kashio": round(no_conciliados_kashio["MONTO"].sum(), 2)
        }

        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")

        # Mover archivos y obtener las rutas finales
        # Kashio
        new_kashio_key = kashio_key.replace("/output/", "/output/processed/", 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={"Bucket": Config.S3_BUCKET, "Key": kashio_key},
            Key=new_kashio_key,
        )
        delete_file_from_s3(kashio_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace("/output/", "/output/processed/", 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={"Bucket": Config.S3_BUCKET, "Key": calimaco_key},
            Key=new_calimaco_key,
        )
        delete_file_from_s3(calimaco_key)

        # Enviar correo
        print("[INFO] Enviando correo con resultados")
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key, metricas, period_email)
        
        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # Insertar en la base de datos las rutas finales
        with next(get_dts_session()) as session:
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
            
            
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True
    
    except Exception as e:
        print(f"[ERROR] Error en conciliation_data: {e}")
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
        
        # insertar datos en base de datos
        with next(get_dts_session()) as session:
            bulk_upsert_collector_records_optimized(session, df2, 1)  

        with next(get_dts_session()) as session:
            bulk_upsert_calimaco_records_optimized(session, df1, 1)  

        # actualizar timestamp del collector
        with next(get_dts_session()) as session:
            update_collector_timestamp(session, 1) 

        # eliminar archivos procesados
        delete_file_from_s3(kashio_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso de actualizacion completado")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_kashio: {e}")
        return False
