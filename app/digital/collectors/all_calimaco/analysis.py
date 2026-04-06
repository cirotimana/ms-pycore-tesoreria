import os
import pandas as pd
import pytz
from datetime import datetime, timedelta
from io import BytesIO
from app.common.s3_utils import read_file_from_s3, upload_file_to_s3, delete_file_from_s3
from app.digital.collectors.calimaco.main import get_main_data

def get_data_calimaco_all(from_date, to_date):
    # obtiene datos de calimaco
    try:
        methods = {
            "kashio": "KASHIO",
            "kushki": "KUSHKI,KUSHKI_TRANSFER_IN",
            "monnet": "MONNET,MONNET_QR",
            "niubiz": "NIUBIZ",
            "nuvei": "NUVEI",
            "pagoefectivo": "PAGOEFECTIVOQR,PAGOEFECTIVO",
            "safetypay": "SAFETYPAY",
            "tupay": "TUPAY_QR,TUPAY",
            "yape": "NIUBIZ_YAPE"
        }
        
        # particionar las fechas en intervalos de 10 dias
        chunks = []
        cur_date = from_date
        while cur_date <= to_date:
            next_date = cur_date + timedelta(days=9)
            if next_date > to_date:
                next_date = to_date
            chunks.append((cur_date, next_date))
            cur_date = next_date + timedelta(days=1)
            
        print(f"[info] particiones generadas:")
        for idx, (st, en) in enumerate(chunks):
            print(f"  > peticion {idx+1}: {st.date()} al {en.date()}")

        local_output_dir = os.path.join(os.getcwd(), "downloads_calimaco")
        os.makedirs(local_output_dir, exist_ok=True)

        for collector, method in methods.items():
            print(f"\n=========================================")
            print(f"🚀 INICIANDO RECAUDADOR: {collector.upper()}")
            print(f"=========================================")
            
            dataframes = []
            
            for idx, (start_dt, end_dt) in enumerate(chunks):
                print(f"\n--- {collector.upper()} | solicitando fecha {idx+1}/{len(chunks)} ({start_dt.date()} - {end_dt.date()}) ---")
                
                # usa el flujo normal por s3 de main.py (sincrono)
                # esto esperara la propia cola natural de la app
                calimaco_key = get_main_data(start_dt, end_dt, method, collector)
                
                if not calimaco_key:
                    print(f"[warn] no se obtuvo key s3 para {collector} en este tramo")
                    continue

                calimaco_content = read_file_from_s3(calimaco_key)
                if not calimaco_content:
                    print(f"[warn] archivo s3 vacio o inexistente para {calimaco_key}")
                    continue

                try:
                    df = pd.read_csv(BytesIO(calimaco_content), encoding="utf-8", low_memory=False, dtype={"ID": str, "Usuario": str, "ID externo": str})
                    if not df.empty:
                        dataframes.append(df)
                    else:
                        print(f"[info] archivo sin registros validos devuetos")
                except Exception as e:
                    print(f"[error] fallo al leer csv alojado s3 de {calimaco_key}: {e}")
                    
                # limpiar temp input original para no acaparar s3
                delete_file_from_s3(calimaco_key)

            if dataframes:
                final_df = pd.concat(dataframes, ignore_index=True)
                
                if 'Estado' in final_df.columns:
                    valids = final_df[final_df['Estado'] == 'Válido']
                else:
                    valids = final_df
                    
                if 'ID' in valids.columns:
                    valids_without_duplicates = valids.drop_duplicates(subset=['ID'], keep='first')
                else:
                    valids_without_duplicates = valids
                
                current_time = datetime.now(pytz.timezone("America/Lima")).strftime("%Y%m%d%H%M%S")
                output_key = f"digital/collectors/{collector}/calimaco/output/Calimaco_{collector.upper()}_Ventas_{current_time}.csv"

                # subir a s3 destino
                with BytesIO() as buffer:
                    valids_without_duplicates.to_csv(buffer, index=False)
                    buffer.seek(0)
                    upload_file_to_s3(buffer.getvalue(), output_key)
                print(f"[ok] {collector} archivo s3 escrito en -> {output_key}")
                
                # descargar a local a la par
                local_path = os.path.join(local_output_dir, f"operaciones_calimaco_{collector}_validas.csv")
                valids_without_duplicates.to_csv(local_path, index=False, encoding='utf-8')
                print(f"[ok] {collector} archivo local -> {local_path} | total registros unificados: {len(valids_without_duplicates)}")
                
            else:
                print(f"[alerta] sin consolidado, omitido en el mes para {collector.upper()}")
                
        return True

    except Exception as e:
        print(f"[error] error general get_data_calimaco: {e}")
        return False

# se permite usarlo directamente de la consola para marzo
if __name__ == '__main__':
    from_date = datetime(2026, 3, 1)
    to_date = datetime(2026, 3, 31)
    get_data_calimaco_all(from_date, to_date)
