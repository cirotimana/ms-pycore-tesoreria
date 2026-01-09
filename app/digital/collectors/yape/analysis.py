import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.yape.utils import *
from app.digital.collectors.yape.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.config import Config
from app.digital.collectors.calimaco.main import *


def get_data_yape(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main_2(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de Yape: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/yape/input/"
        s3_files = list_files_in_s3(s3_prefix)

        dataframes = []

        for s3_key in s3_files:
            if (s3_key.endswith('.csv')) and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                  
                    with BytesIO(content) as csv_data:
                        df = pd.read_csv(csv_data, header=3, dtype={'N°Voucher/Id pedido': str, 'ID operación': str})
                    
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
                    print(f"Error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)

            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/yape/output/Yape_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)

            # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
            print(f"[SUCCESS] Yape procesado exitosamente: {output_key}")
            return True
        
        else:
            print("[✖] No se encontraron archivos CSV para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos yape: {e}")
        return False


def get_data_calimaco(from_date, to_date):
    try:
        method = 'NIUBIZ_YAPE'
        collector = 'yape'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Yape_Ventas_{current_time}.csv"
        
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
    
##comparar de yape Nro Pedido y de calimaco Número de compra
def conciliation_data(from_date, to_date):
    try:
        # archivos de donde se alimentaran los df
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/yape/calimaco/output/Calimaco_Yape_Ventas_"
        yape_prefix = "digital/collectors/yape/output/Yape_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        yape_key = get_latest_file_from_s3(yape_prefix)

        if not calimaco_key or not yape_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Yape: {yape_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        yape_content = read_file_from_s3(yape_key)

        df1 = pd.read_csv(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(yape_content), dtype={'N°Voucher/Id pedido': str, 'ID operación': str})

        
        df2 = df2.rename(columns={'Fecha y hora de Transacción':'FECHA'})
        df2 = df2.rename(columns={'N°Voucher/Id pedido':'ID CALIMACO'})
        df2 = df2.rename(columns={'ID operación':'ID PROVEEDOR'})
        df2['CLIENTE'] = '-'
        df2 = df2.rename(columns={'Monto':'MONTO'})
        df2 = df2.rename(columns={'Tipo operación':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]

        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 5)  
            bulk_upsert_calimaco_records_optimized(session, df1, 5) 
        
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
        
        cols_yape = [
            "FECHA",
            "ID CALIMACO",
            "ID PROVEEDOR",
            "CLIENTE",
            "MONTO",
            "ESTADO PROVEEDOR",
        ]

        
        # Condicion 1
        df1_cond1 = df1[df1['Estado'].isin(['Denegado', 'Nuevo', 'CANCELLED', 'Límites excedidos' ])]
        df2_cond1 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        df2_cond1.loc[:, 'ID CALIMACO'] = df2_cond1['ID CALIMACO'].astype(str).str[2:]
        conciliacion_cond1 = pd.merge(
        df1_cond1.assign(Numero_compra_temp=df1_cond1['ID'].str[2:]),
        df2_cond1,
        left_on='Numero_compra_temp',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False).drop('Numero_compra_temp', axis=1)

        # condicion 2: ambos aprobados
        df1_cond2 = df1[df1['Estado'] == 'Válido']
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        df2_cond2.loc[:, 'ID CALIMACO'] = df2_cond2['ID CALIMACO'].astype(str).str[2:]
        conciliacion_cond2 = pd.merge(
        df1_cond2.assign(Numero_compra_temp=df1_cond2['ID'].str[2:]),
        df2_cond2,
        left_on='Numero_compra_temp',
        right_on='ID CALIMACO',
        how='inner',
        indicator=False).drop('Numero_compra_temp', axis=1)
        
        
        #condicion 3 _ duplicados yape
        df2_cond3 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        duplicados_df2 = df2_cond3[df2_cond3.duplicated(subset=['ID CALIMACO'], keep=False)]
        
        
        # condicion 4 _ registros aprovados que no hicieron match
        df1_cond4 = df1[df1['Estado'] == 'Válido']
        df2_cond4 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        df2_cond4.loc[:, 'ID CALIMACO'] = df2_cond4['ID CALIMACO'].astype(str).str[2:]
        no_match = pd.merge(
        df1_cond4.assign(Numero_compra_temp=df1_cond4['ID'].str[2:]),
        df2_cond4,
        left_on='Numero_compra_temp',
        right_on='ID CALIMACO',
        how='outer',
        indicator=True).drop('Numero_compra_temp', axis=1)
        
        # condicion 5 _ original
        df2_original = df2.copy()
        
        no_match = no_match.rename(columns={'_merge': 'Recaudador Aprobado'})
        # Cambiar valores
        no_match['Recaudador Aprobado'] = no_match['Recaudador Aprobado'].cat.rename_categories({
            'left_only': 'Calimaco Aprobado',
            'right_only': 'Yape Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Yape Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_yape = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Yape Aprobado']
        no_conciliados_yape = no_conciliados_yape[cols_yape]
        

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Yape_Conciliacion_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            conciliacion_cond2.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        
        output_key_re = f"digital/apps/total-secure/conciliaciones/processed/Yape_Conciliacion_Ventas_{current_time}.xlsx"
        
        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                save_dfs_to_excel(writer, {
                    "Operaciones Conciliadas": conciliacion_cond2,
                    "No Conciliados Calimaco": no_conciliados_calimaco,
                    "No Conciliados Proveedor": no_conciliados_yape,
                    "Operaciones Duplicadas": duplicados_df2,
                    "Cambios de Estado": conciliacion_cond1,
                    "Proveedor Original": df2_original
                })
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key_re)

        
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_csv(BytesIO(conciliacion_content), encoding='utf-8', low_memory=False)
        
        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        # approvals_df_yape = df2[df2['ESTADO PROVEEDOR'].isin (['Depositada'])]
        approvals_df_yape = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]

                

        metricas = {
            "total_calimaco": len(df1),
            "total_yape": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_yape": len(approvals_df_yape),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_yape": round(approvals_df_yape['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_yape": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_yape": len(no_conciliados_yape),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_yape": round(no_conciliados_yape["MONTO"].sum(), 2)
        }
        
        print("Datos obtenidos:")
        for k, v in metricas.items():
            print(f"- {k}: {v}")


        # Mover archivos y obtener las rutas finales
        # Yape
        new_yape_key = yape_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': yape_key},
            Key=new_yape_key
        )
        delete_file_from_s3(yape_key)

        # Calimaco
        new_calimaco_key = calimaco_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': calimaco_key},
            Key=new_calimaco_key
        )
        delete_file_from_s3(calimaco_key)
        
        # Enviamos el correo con los adjuntos
        print("[INFO] enviando correo con resultados")
        period_email = f"{from_date.strftime("%Y/%m/%d")} - {to_date.strftime("%Y/%m/%d")}"
        send_email_with_results(output_key_re, metricas, period_email)    
        
        

        # convierte ambas a date (YYYY-MM-DD)
        from_date_fmt = from_date.date()
        to_date_fmt = to_date.date()  

        # Insertar en la base de datos las rutas finales de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                5,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_yape"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_yape"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_yape"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_yape"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_yape_key}"
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
        print(f"[✖] Error en conciliation_data para yape: {e}")
        return False
        
        
def updated_data_yape():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/yape/calimaco/output/Calimaco_Yape_Ventas_"
        yape_prefix = "digital/collectors/yape/output/Yape_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        yape_key = get_latest_file_from_s3(yape_prefix)

        if not calimaco_key or not yape_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Yape: {yape_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        yape_content = read_file_from_s3(yape_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(yape_content), dtype={'Nro Pedido': str})
        
        df2 = df2[df2['Estado'].isin(['Autorizada', 'Liquidada'])]
        df2['Estado'] = df2['Estado'].replace({
            'Autorizada': 'Venta',
            'Liquidada': 'Venta'
        })
        
        # aplicar formato y cambio de nombres de columnas
        df2 = df2.rename(columns={'Fecha de Transacción':'FECHA'})
        df2 = df2.rename(columns={'Nro Pedido':'ID CALIMACO'})
        # df2 = df2.rename(columns={'ID operación':'ID PROVEEDOR'})
        df2['ID PROVEEDOR'] = '-'
        df2 = df2.rename(columns={'Cliente':'CLIENTE'})
        df2 = df2.rename(columns={'Importe Pedido':'MONTO'})
        df2 = df2.rename(columns={'Estado':'ESTADO PROVEEDOR'})
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # filtrar y eliminar duplicados
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 5) 
            bulk_upsert_calimaco_records_optimized(session, df1, 5)  
            update_collector_timestamp(session, 5) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(yape_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso exitoso para la actualizacion de Yape")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_yape: {e}")
        return False


def get_data_yape_1(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main_json(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de Yape: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/yape/input/"
        s3_files = list_files_in_s3(s3_prefix)

        dataframes = []

        for s3_key in s3_files:
            if (s3_key.endswith('.csv')) and '/input/processed/' not in s3_key:
                try:
                    content = read_file_from_s3(s3_key)
                  
                    with BytesIO(content) as csv_data:
                        df = pd.read_csv(csv_data, dtype={'Nro Pedido': str})
                    
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
                    print(f"Error al procesar {s3_key}: {e}")

        if dataframes:
            consolidated_df = pd.concat(dataframes, ignore_index=True)

            # Guardar en S3
            current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
            output_key = f"digital/collectors/yape/output/Yape_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)

            # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
            print(f"[SUCCESS] Yape procesado exitosamente: {output_key}")
            return True
        
        else:
            print("[✖] No se encontraron archivos CSV para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos yape: {e}")
        return False


if __name__ == "__main__":
    updated_data_yape()
