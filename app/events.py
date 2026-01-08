from fastapi_utilities import repeat_at
import asyncio
from app.utils.endpoint_lock import endpoint_lock 


# CANAL DIGITAL
from app.digital.collectors.kashio.main import get_main_kashio, get_updated_kashio
from app.digital.collectors.kashio.liquidations.main import get_main_kashio_liq
from app.digital.collectors.monnet.main import get_main_monnet, get_updated_monnet
from app.digital.collectors.kushki.main import get_main_kushki, get_updated_kushki
from app.digital.collectors.yape.main import get_main_yape, get_updated_yape
from app.digital.collectors.niubiz.main import get_main_niubiz, get_updated_niubiz
from app.digital.collectors.nuvei.main import get_main_nuvei, get_updated_nuvei
from app.digital.collectors.pagoefectivo.main import get_main_pagoefectivo, get_updated_pagoefectivo
from app.digital.collectors.pagoefectivo.liquidations.main import get_main_pagoefectivo_liq
from app.digital.collectors.safetypay.main import get_main_safetypay, get_updated_safetypay
from app.digital.collectors.tupay.main import get_main_tupay, get_updated_tupay
from app.digital.collectors.tupay.liquidations.main import get_main_tupay_liq


from app.config import Config


    
##   CRON DIGITAL   ##  
 

# ============================================
# ACTUALIZACIONES RECAUDADORES (CADA HORA)
# CON SISTEMA DE BLOQUEO
# ============================================
# IMPORTANTEEEEE: Estos crons comparten el mismo lock con sus endpoints

@repeat_at(cron="0 * * * *")
@endpoint_lock("kashio-process")  
def processing_data_with_cron_updated_kashio():
    try:
        print("Ejecutando actualizacion para kashio...")
        get_updated_kashio()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Kashio update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated kashio: {e}")

@repeat_at(cron="2 * * * *")
@endpoint_lock("monnet-process")  
def processing_data_with_cron_updated_monnet():
    try:
        print("Ejecutando actualizacion para monnet...")
        get_updated_monnet()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Monnet update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated monnet: {e}")

@repeat_at(cron="4 * * * *")
@endpoint_lock("kushki-process")  
def processing_data_with_cron_updated_kushki():
    try:
        print("Ejecutando actualizacion para kushki...")
        get_updated_kushki()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Kushki update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated kushki: {e}")

@repeat_at(cron="6 * * * *")
@endpoint_lock("niubiz-process")  
def processing_data_with_cron_updated_niubiz():
    try:
        print("Ejecutando actualizacion para niubiz...")
        get_updated_niubiz()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Niubiz update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated niubiz: {e}")

@repeat_at(cron="8 * * * *")
@endpoint_lock("yape-process")  
def processing_data_with_cron_updated_yape():
    try:
        print("Ejecutando actualizacion para yape...")
        get_updated_yape()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Yape update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated yape: {e}")

@repeat_at(cron="10 * * * *")
@endpoint_lock("nuvei-process")  
def processing_data_with_cron_updated_nuvei():
    try:
        print("Ejecutando actualizacion para nuvei...")
        asyncio.run(get_updated_nuvei())
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Nuvei update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated nuvei: {e}")

@repeat_at(cron="12 * * * *")
@endpoint_lock("pagoefectivo-process")  
def processing_data_with_cron_updated_pagoefectivo():
    try:
        print("Ejecutando actualizacion para pagoefectivo...")
        get_updated_pagoefectivo()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"PagoEfectivo update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated pagoefectivo: {e}")

@repeat_at(cron="14 * * * *")
@endpoint_lock("safetypay-process")  
def processing_data_with_cron_updated_safetypay():
    try:
        print("Ejecutando actualizacion para safetypay...")
        get_updated_safetypay()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"SafetyPay update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated safetypay: {e}")

@repeat_at(cron="16 * * * *")
@endpoint_lock("tupay-process")  
def processing_data_with_cron_updated_tupay():
    try:
        print("Ejecutando actualizacion para tupay...")
        get_updated_tupay()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Tupay update bloqueado: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron updated tupay: {e}")


# ============================================
# CONCILIACIONES RECAUDADORES
# CON SISTEMA DE BLOQUEO
# ============================================

##todos los dias a las 05:30
@repeat_at(cron="30 5 * * *")
@endpoint_lock("kashio-process")  
def processing_data_with_cron_getkashio():
    try:
        print("Ejecutando conciliacion para kashio...")
        get_main_kashio()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Kashio conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getkashio: {e}")

##todos los dias a las 05:00
@repeat_at(cron="0 5 * * *")
@endpoint_lock("monnet-process")  
def processing_data_with_cron_getmonnet():
    try:
        print("Ejecutando conciliacion para monnet...")
        get_main_monnet()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Monnet conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getmonnet: {e}")
    
##todos los dias a las 06:00
@repeat_at(cron="0 6 * * *")
@endpoint_lock("kushki-process")  
def processing_data_with_cron_getkushki():
    try:
        print("Ejecutando conciliacion para kushki...")
        get_main_kushki()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Kushki conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getkushki: {e}")

##todos los dias a las 06:30
@repeat_at(cron="30 6 * * *")
@endpoint_lock("niubiz-process")  
def processing_data_with_cron_getniubiz():
    try:
        print("Ejecutando conciliacion para niubiz...")
        get_main_niubiz()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Niubiz conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getniubiz: {e}")

##todos los dias a las 07:00
@repeat_at(cron="0 7 * * *")
@endpoint_lock("yape-process")  
def processing_data_with_cron_getyape():
    try:
        print("Ejecutando conciliacion para yape...")
        get_main_yape()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Yape conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getyape: {e}")
    
##todos los dias a las 07:30
@repeat_at(cron="30 7 * * *")
@endpoint_lock("nuvei-process")  
def processing_data_with_cron_getnuvei():
    try:
        print("Ejecutando conciliacion para nuvei...")
        asyncio.run(get_main_nuvei())
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Nuvei conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getnuvei: {e}")
    
##todos los dias a las 08:00
@repeat_at(cron="0 8 * * *")
@endpoint_lock("pagoefectivo-process")  
def processing_data_with_cron_getpagoefectivo():
    try:
        print("Ejecutando conciliacion para pagoefectivo...")
        get_main_pagoefectivo()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"PagoEfectivo conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getpagoefectivo: {e}")
    
##todos los dias a las 08:30
@repeat_at(cron="30 8 * * *")
@endpoint_lock("safetypay-process")  
def processing_data_with_cron_getsafetypay():
    try:
        print("Ejecutando conciliacion para safetypay...")
        get_main_safetypay()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"SafetyPay conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron getsafetypay: {e}")
    
##todos los dias a las 09:00
@repeat_at(cron="0 9 * * *")
@endpoint_lock("tupay-process")  
def processing_data_with_cron_gettupayy():
    try:
        print("Ejecutando conciliacion para tupay...")
        get_main_tupay()
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Tupay conciliacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en cron gettupay: {e}")


# ============================================
# LIQUIDACIONES DE RECAUDADORES
# ============================================

# todos los dias a las 03:30 am
@repeat_at(cron="30 3 * * *")
@endpoint_lock("kashio-process-liq")
def cron_liquidation_kashio():
    try:
        print("Ejecutando proceso de liquidacion para Kashio")
        result = get_main_kashio_liq()
        if result:
            print(f"Resultado exitoso, archivo guardado en {result}")
        else:
            print("Falla en el proceso")
            return
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Kashio liquidacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en el cron de liq kashio: {e}")
        
# todos los dias a las 4:15 am
@repeat_at(cron="15 4 * * *")
@endpoint_lock("tupay-process-liq")
def cron_liquidation_tupay():
    try:
        print("Ejecutando proceso de liquidacion para Tupay")
        result = get_main_tupay_liq()
        if result:
            print(f"Resultado exitoso, archivo guardado en {result}")
        else:
            print("Falla en el proceso")
            return
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Tupay liquidacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en el cron de liq Tupay: {e}")

# todos los dias a las 4:30 am
@repeat_at(cron="30 4 * * *")
@endpoint_lock("pagoefectivo-process-liq")
def cron_liquidation_pagoefectivo():
    try:
        print("Ejecutando proceso de liquidacion para Pagoefectivo")
        result = get_main_pagoefectivo_liq()
        if result:
            print(f"Resultado exitoso, archivo guardado en {result}")
        else:
            print("Falla en el proceso")
            return
    except Exception as e:
        if hasattr(e, 'status_code') and e.status_code == 409:
            print(f"Pagoefectivo liquidacion bloqueada: {e.detail.get('message', 'Ya en ejecucion')}")
        else:
            print(f"Error en el cron de liq Pagoefectivo: {e}")

