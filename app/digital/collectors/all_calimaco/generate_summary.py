import os
import pandas as pd

def generate_summary():
    # ruta de la carpeta que contiene los archivos
    folder_path = r"C:\Users\ciro.timana\Documents\GitHub\ms-pycore-tesoreria\downloads_calimaco"
    output_excel_path = os.path.join(folder_path, "resumen_calimaco.xlsx")

    # leer todos los archivos csv de calimaco validos
    csv_files_list = [f for f in os.listdir(folder_path) if f.startswith("operaciones_calimaco_") and f.endswith("_validas.csv")]
    
    # listas para almacenar la informacion de todas las hojas
    general_summary_data = [] 
    collectors_dataframes_dict = {} 
    
    print(f"[info] buscando archivos en {folder_path} ... encontro {len(csv_files_list)} archivos.")

    for file_name in csv_files_list:
        # extraer el nombre limpio de cada recaudador
        collector_name = file_name.replace("operaciones_calimaco_", "").replace("_validas.csv", "").upper()
        file_path = os.path.join(folder_path, file_name)
        
        print(f"[info] agrupando informacion de {collector_name}...")
        
        try:
            # solo se leen cantidad y fecha de modificacion para optimizar la memoria ram ya que hay algunos muy grandes
            dataframe = pd.read_csv(file_path, usecols=['Fecha de modificación', 'Cantidad'], low_memory=False)
        except Exception as e:
            print(f"[error] al leer {file_name}: {e}")
            continue
            
        # eliminar filas vacias en estas columnas criticas
        dataframe = dataframe.dropna(subset=['Fecha de modificación', 'Cantidad'])
        
        # convertir formato de strings a fechas y capturar unicamente el dia exacto
        dataframe['Fecha de modificación'] = pd.to_datetime(dataframe['Fecha de modificación'], errors='coerce').dt.date
        dataframe['Cantidad'] = pd.to_numeric(dataframe['Cantidad'], errors='coerce').fillna(0)
        
        # agrupar sumando las cantidades en cada dia
        grouped_dataframe = dataframe.groupby('Fecha de modificación', dropna=True)['Cantidad'].sum().reset_index()
        grouped_dataframe = grouped_dataframe.sort_values(by='Fecha de modificación')
        
        # nombrar columnas limpias de presentacion
        grouped_dataframe = grouped_dataframe.rename(columns={'Fecha de modificación': 'FECHA', 'Cantidad': 'TOTAL'})
        
        # sumar toda la cantidad acumulada de este recaudador
        total_amount = grouped_dataframe['TOTAL'].sum()
        
        # añadir la fila del total al final de este dataframe agrupado
        total_row_dataframe = pd.DataFrame([{'FECHA': 'TOTAL GENERAL', 'TOTAL': total_amount}])
        final_collector_dataframe = pd.concat([grouped_dataframe, total_row_dataframe], ignore_index=True)
        
        collectors_dataframes_dict[collector_name] = final_collector_dataframe
        
        # guardar el total del mes en la memoria del resumen de la sabana uno
        general_summary_data.append({'RECAUDADOR': collector_name, 'MONTO MENSUAL': total_amount})

    # crear dataframe con la estructura de primera pagina resumen
    summary_dataframe = pd.DataFrame(general_summary_data)
    total_all_collectors = summary_dataframe['MONTO MENSUAL'].sum()
    
    # anexar el mega total de la suma de todos los recaudadores
    total_all_dataframe = pd.DataFrame([{'RECAUDADOR': 'TOTAL GENERAL RECAUDADORES', 'MONTO MENSUAL': total_all_collectors}])
    summary_dataframe = pd.concat([summary_dataframe, total_all_dataframe], ignore_index=True)
    
    print(f"[info] exportando a excel final: {output_excel_path} (espere unos segundos)")
    
    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as excel_writer:
        # generar hoja al principio
        summary_dataframe.to_excel(excel_writer, sheet_name='RESUMEN', index=False)
        worksheet_summary = excel_writer.sheets['RESUMEN']
        worksheet_summary.set_column('A:A', 30)
        worksheet_summary.set_column('B:B', 25)
        
        # crear las pestañas subsiguientes iterando en diccionario
        for collector_key, df_collector_values in collectors_dataframes_dict.items():
            # excel restringe hojas con nombres largos
            safe_sheet_name = collector_key[:31] 
            df_collector_values.to_excel(excel_writer, sheet_name=safe_sheet_name, index=False)
            worksheet_collector = excel_writer.sheets[safe_sheet_name]
            worksheet_collector.set_column('A:A', 20)
            worksheet_collector.set_column('B:B', 20)
            
    print(f"[ok] resumen consolidado y multioja guardado exitosamente en: {output_excel_path}")

if __name__ == "__main__":
    generate_summary()
