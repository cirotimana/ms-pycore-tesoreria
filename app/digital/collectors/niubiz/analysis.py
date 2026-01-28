import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.niubiz.utils import *
from app.digital.collectors.niubiz.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *


def get_data_niubiz(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main_2(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de Niubiz: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/niubiz/input/"
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
            output_key = f"digital/collectors/niubiz/output/Niubiz_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)

            # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
            print(f"[SUCCESS] Niubiz procesado exitosamente: {output_key}")
            return True
        
        else:
            print("[✖] No se encontraron archivos CSV para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos niubiz: {e}")
        return False


def get_data_calimaco(from_date, to_date):
    try:
        method = 'NIUBIZ'
        collector = 'niubiz'
        calimaco_key = get_main_data(from_date, to_date, method, collector)
        calimaco_content = read_file_from_s3(calimaco_key)

        df = pd.read_csv(BytesIO(calimaco_content), encoding='utf-8', low_memory=False, dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        
        valids = df[df['Estado'] == 'Válido']
        other_states =  df[df['Estado'] != 'Válido']
        valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
        
        df = pd.concat([valids_without_duplicates, other_states], ignore_index=True) 

        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_Niubiz_Ventas_{current_time}.csv"
        
        with BytesIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
            
        delete_file_from_s3(calimaco_key)
        
        ##download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
        print(f"[SUCCESS] Calimaco procesado exitosamente: {output_key}")
        return True
    
    except Exception as e:
        print(f"[✖] Error en get_data_calimaco: {e}")
        return False


##comparar de niubiz Nro Pedido y de calimaco Número de compra
def conciliation_data(from_date, to_date):
    try:
        
        # archivos de donde se alimentaran los df
        s3_client = get_s3_client_with_role()
        calimaco_prefix = "digital/collectors/niubiz/calimaco/output/Calimaco_Niubiz_Ventas_"
        niubiz_prefix = "digital/collectors/niubiz/output/Niubiz_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        niubiz_key = get_latest_file_from_s3(niubiz_prefix)

        if not calimaco_key or not niubiz_key:
            print("[ALERTA] No se encontraron archivos para conciliar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Niubiz: {niubiz_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        niubiz_content = read_file_from_s3(niubiz_key)

        # df1 = pd.read_csv(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        # df2 = pd.read_csv(BytesIO(niubiz_content), dtype={'N°Voucher/Id pedido': str, 'ID operación': str})

        # df2 = df2.rename(columns={'Fecha y hora de Transacción':'FECHA'})
        # df2 = df2.rename(columns={'N°Voucher/Id pedido':'ID CALIMACO'})
        # df2 = df2.rename(columns={'ID operación':'ID PROVEEDOR'})
        # df2['CLIENTE'] = '-'
        # df2 = df2.rename(columns={'Monto':'MONTO'})
        # df2 = df2.rename(columns={'Tipo operación':'ESTADO PROVEEDOR'})
        
        # # Limpiar espacios en ID CALIMACO
        # df2['ID CALIMACO'] = df2['ID CALIMACO'].astype(str).str.strip()
        
        # df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        # df1['Data'] = "<==>"
        # df2=df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR",]]
        
        # df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        # df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')

        df1 = pd.read_csv(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(niubiz_content), dtype={'Nro Pedido': str})
        
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
        
        # limpiar espacios en ID CALIMACO
        df2['ID CALIMACO'] = df2['ID CALIMACO'].astype(str).str.strip()
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df1['Data'] = "<==>"
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # eliminar duplicados
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        
        # Insertar datos del collector y Calimaco de forma dual
        def initial_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 4) 
            bulk_upsert_calimaco_records_optimized(session, df1, 4)      
        
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
        
        cols_niubiz = [
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
        # df2_cond1.loc[:, 'ID CALIMACO'] = df2_cond1['ID CALIMACO'].astype(str).str[2:]
        conciliacion_cond1 = pd.merge(
        df1_cond1.assign(Numero_compra_temp=df1_cond1['ID'].str[2:]),
        df2_cond1,
        # left_on='Numero_compra_temp',
        # right_on='ID CALIMACO',
        left_on=['Numero_compra_temp', 'Cantidad'],
        right_on=['ID CALIMACO', 'MONTO'],
        how='inner',
        indicator=False).drop('Numero_compra_temp', axis=1)

        # condicion 2: ambos aprobados
        df1_cond2 = df1[df1['Estado'] == 'Válido']
        df2_cond2 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        # df2_cond2.loc[:, 'ID CALIMACO'] = df2_cond2['ID CALIMACO'].astype(str).str[2:]
        conciliacion_cond2 = pd.merge(
        df1_cond2.assign(Numero_compra_temp=df1_cond2['ID'].str[2:]),
        df2_cond2,
        # left_on='Numero_compra_temp',
        # right_on='ID CALIMACO',
        left_on=['Numero_compra_temp', 'Cantidad'],
        right_on=['ID CALIMACO', 'MONTO'],
        how='inner',
        indicator=False).drop('Numero_compra_temp', axis=1)
        
        
        #condicion 3 _ duplicados niubiz
        df2_cond3 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
        duplicados_df2 = df2_cond3[df2_cond3.duplicated(subset=['ID CALIMACO'], keep=False)]
        
        
        # condicion 4 _ registros aprovados que no hicieron match
        df1_cond4 = df1[df1['Estado'] == 'Válido']
        df2_cond4 = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]
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
            'right_only': 'Niubiz Aprobado',
            'both': 'Ambos'
        })
        # Filtrar solo los que están solo en uno de los dos
        no_match_filtrado = no_match[no_match['Recaudador Aprobado'].isin(['Calimaco Aprobado', 'Niubiz Aprobado'])]
        
        no_conciliados_calimaco = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Calimaco Aprobado']
        no_conciliados_calimaco = no_conciliados_calimaco[cols_calimaco]
        no_conciliados_niubiz = no_match_filtrado[no_match_filtrado['Recaudador Aprobado'] == 'Niubiz Aprobado']
        no_conciliados_niubiz = no_conciliados_niubiz[cols_niubiz]

        # Guardar resultado en S3
        current_time = datetime.now(pytz.timezone("America/Lima")).strftime('%Y%m%d%H%M%S')
        output_key = f"digital/apps/total-secure/conciliaciones/processed/Niubiz_Conciliacion_Ventas_{current_time}.xlsx"

        with BytesIO() as buffer:
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                conciliacion_cond2.to_excel(writer, sheet_name='Operaciones Conciliadas', index=False)
                no_conciliados_calimaco.to_excel(writer, sheet_name='No Conciliados Calimaco', index=False)
                no_conciliados_niubiz.to_excel(writer, sheet_name='No Conciliados Proveedor', index=False)
                duplicados_df2.to_excel(writer, sheet_name='Operaciones Duplicadas', index=False)
                conciliacion_cond1.to_excel(writer, sheet_name='Cambios de Estado', index=False)
                df2_original.to_excel(writer, sheet_name='Proveedor Original', index=False)
            buffer.seek(0)
            upload_file_to_s3(buffer.getvalue(), output_key)
        
        ##download_file_from_s3_to_local(output_key)##para pruebitas lo guardo en local
        
        conciliacion_content = read_file_from_s3(output_key)
        conciliadas_df = pd.read_excel(BytesIO(conciliacion_content), sheet_name="Operaciones Conciliadas")

        approvals_df_calimaco = df1[df1['Estado'] == 'Válido']
        approvals_df_niubiz = df2[df2['ESTADO PROVEEDOR'].isin(['Venta'])]


        metricas = {
            "total_calimaco": len(df1),
            "total_niubiz": len(df2),
            "aprobados_calimaco": len(approvals_df_calimaco),
            "aprobados_niubiz": len(approvals_df_niubiz),
            "recaudacion_calimaco": round(approvals_df_calimaco['Cantidad'].sum(), 2),
            "recaudacion_niubiz": round(approvals_df_niubiz['MONTO'].sum(), 2),
            "conciliados_total": len(conciliadas_df),
            "conciliados_monto_calimaco": round(conciliadas_df["Cantidad"].sum(), 2),
            "conciliados_monto_niubiz": round(conciliadas_df["MONTO"].sum(), 2),
            "no_conciliados_calimaco": len(no_conciliados_calimaco),
            "no_conciliados_niubiz": len(no_conciliados_niubiz),
            "no_conciliados_monto_calimaco": round(no_conciliados_calimaco["Cantidad"].sum(), 2),
            "no_conciliados_monto_niubiz": round(no_conciliados_niubiz["MONTO"].sum(), 2)
        }
        
        print("[INFO] Datos obtenidos")
        
        for k , v in metricas.items():
            print(f"- {k}: {v}")

        # Mover archivos y obtener las rutas finales
        # niubiz
        new_niubiz_key = niubiz_key.replace('/output/', '/output/processed/', 1)
        s3_client.copy_object(
            Bucket=Config.S3_BUCKET,
            CopySource={'Bucket': Config.S3_BUCKET, 'Key': niubiz_key},
            Key=new_niubiz_key
        )
        delete_file_from_s3(niubiz_key)

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

        ## Insertar en la base de datos las rutas finales de forma dual
        def final_save(session):
            conciliation_id = insert_conciliations(
                4,
                session,
                1,
                from_date_fmt,
                to_date_fmt,
                metricas["recaudacion_calimaco"],
                metricas["recaudacion_niubiz"],
                metricas["aprobados_calimaco"],
                metricas["aprobados_niubiz"],
                metricas["no_conciliados_calimaco"],
                metricas["no_conciliados_niubiz"],
                metricas["no_conciliados_monto_calimaco"],
                metricas["no_conciliados_monto_niubiz"],
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_calimaco_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 1, f"s3://{Config.S3_BUCKET}/{new_niubiz_key}"
            )
            insert_conciliation_files(
                session, conciliation_id, 2, f"s3://{Config.S3_BUCKET}/{output_key}"
            )
            session.commit()

        run_on_dual_dts(final_save)
                
        print(f"[SUCCESS] Conciliacion completada exitosamente: {output_key}")
        return True 
    
    except Exception as e:
        print(f"[✖] Error en conciliation_data para niubiz: {e}")
        return False


def updated_data_niubiz():
    try:
        # procesar archivos descargados
        calimaco_prefix = "digital/collectors/niubiz/calimaco/output/Calimaco_Niubiz_Ventas_"
        niubiz_prefix = "digital/collectors/niubiz/output/Niubiz_Ventas_"

        calimaco_key = get_latest_file_from_s3(calimaco_prefix)
        niubiz_key = get_latest_file_from_s3(niubiz_prefix)

        if not calimaco_key or not niubiz_key:
            print("[ALERTA] No se encontraron archivos para actualizar")
            return False

        print(f"[INFO] Procesando archivo Calimaco: {calimaco_key}")
        print(f"[INFO] Procesando archivo Niubiz: {niubiz_key}")

        # leer archivos directamente desde S3
        calimaco_content = read_file_from_s3(calimaco_key)
        niubiz_content = read_file_from_s3(niubiz_key)
        
        df1 = pd.read_csv(BytesIO(calimaco_content), dtype={'ID': str, 'Usuario': str, 'ID externo': str})
        df2 = pd.read_csv(BytesIO(niubiz_content), dtype={'Nro Pedido': str})
        
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
        
        # limpiar espacios en ID CALIMACO
        df2['ID CALIMACO'] = df2['ID CALIMACO'].astype(str).str.strip()
        
        df1 = df1[["ID","Fecha","Fecha de modificación","Estado","Usuario","Cantidad","ID externo","Comentarios"]]
        df2 = df2[["FECHA","ID CALIMACO","ID PROVEEDOR","CLIENTE","MONTO","ESTADO PROVEEDOR"]]
        
        # eliminar duplicados
        df1 = df1.drop_duplicates(subset=['ID', 'Estado'])
        df2 = df2.drop_duplicates(subset=['ID CALIMACO'], keep='first')
        
        # insertar datos y actualizar timestamp de forma dual
        def update_save(session):
            bulk_upsert_collector_records_optimized(session, df2, 4)  
            bulk_upsert_calimaco_records_optimized(session, df1, 4)  
            update_collector_timestamp(session, 4) 

        run_on_dual_dts(update_save)

        # eliminar archivos procesados
        delete_file_from_s3(niubiz_key)
        delete_file_from_s3(calimaco_key)
        
        print("[SUCCESS] Proceso de actualizacion exitoso")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_niubiz: {e}")
        return False
    
    
def get_data_niubiz_1(from_date, to_date):
    s3_client = get_s3_client_with_role()
    try:
        get_data_main_json(from_date, to_date)
    except Exception as e:
        print(f"[ALERTA] Error ejecutando la descarga de Niubiz: {e}")
        return False

    try:
        s3_prefix = "digital/collectors/niubiz/input/"
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
            output_key = f"digital/collectors/niubiz/output/Niubiz_Ventas_{current_time}.csv"

            with BytesIO() as buffer:
                consolidated_df.to_csv(buffer, index=False)
                buffer.seek(0)
                upload_file_to_s3(buffer.getvalue(), output_key)

            # download_file_from_s3_to_local(output_key)  # Comentado para optimizar
            print(f"[SUCCESS] Niubiz procesado exitosamente: {output_key}")
            return True
        
        else:
            print("[✖] No se encontraron archivos CSV para consolidar.")
            return False

    except Exception as e:
        print(f"[✖] Error procesando datos niubiz: {e}")
        return False


if __name__ == "__main__":
    updated_data_niubiz()
