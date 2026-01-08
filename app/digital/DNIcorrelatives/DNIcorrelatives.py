import sys
import os

# # Agregar el directorio raiz del proyecto al PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.digital.DNIcorrelatives.main import get_main

def get_dnicorrelatives():
    try:
        result = get_main()
        if result:
            print("DNIcorrelatives - Proceso completado exitosamente")
        else:
            print("DNIcorrelatives - Proceso fallo despues de todos los reintentos")
        return result
    except Exception as e:
        print(f"DNIcorrelatives - Error inesperado: {e}")
        return False

if __name__ == "__main__":
    get_dnicorrelatives()