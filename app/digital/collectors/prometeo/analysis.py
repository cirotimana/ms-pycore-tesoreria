import pandas as pd
import pytz
from datetime import datetime
from app.digital.collectors.prometeo.utils import *
from app.digital.collectors.prometeo.email_handler import *
from app.common.database import *
from app.common.database import get_dts_session
from app.common.s3_utils import *
from app.digital.collectors.calimaco.main import *

def get_data_prometeo(from_date, to_date):
    # implementar logica de descarga de prometeo
    print("[INFO] Descarga de Prometeo no implementada")
    return False

def get_data_calimaco(from_date, to_date):
    # implementar logica de descarga de calimaco para prometeo
    print("[INFO] Descarga de Calimaco para Prometeo no implementada")
    return False

def updated_data_prometeo():
    try:
        from datetime import date
        import pytz
        
        # obtener fecha actual con timezone
        lima_tz = pytz.timezone("America/Lima")
        today_date = date.today()
        today_datetime = datetime.combine(today_date, datetime.min.time()).replace(tzinfo=lima_tz)
        
        print(f"[INFO] Ejecutando descarga automatica para fecha: {today_date}")
        
        # ejecutar descarga de prometeo
        print("[INFO] Descargando datos de Prometeo...")
        prometeo_result = get_data_prometeo(today_datetime, today_datetime)
        
        if not prometeo_result:
            print("[ERROR] Fallo la descarga de Prometeo")
            return False
            
        # ejecutar descarga de calimaco
        print("[INFO] Descargando datos de Calimaco...")
        calimaco_result = get_data_calimaco(today_datetime, today_datetime)
        
        if not calimaco_result:
            print("[ERROR] Fallo la descarga de Calimaco")
            return False
        
        # actualizar timestamp del collector de forma dual
        def update_save(session):
            update_collector_timestamp(session, 10)  # 10 = Prometeo (asumiendo ID)
        
        run_on_dual_dts(update_save)
        
        print(f"[SUCCESS] Proceso automatico completado para {today_date}")
        return True
  
    except Exception as e:
        print(f"[ERROR] Error en updated_data_prometeo: {e}")
        return False
