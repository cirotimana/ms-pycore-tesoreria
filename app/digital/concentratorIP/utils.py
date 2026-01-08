import os
import math
import pandas as pd
from difflib import SequenceMatcher

def calculate_similarity(a, b):
    """Calcula la similitud entre dos cadenas"""
    return SequenceMatcher(None, a, b).ratio()

def convert_row_to_json_safe_dict(row):
    """Convierte una fila de DataFrame a un diccionario seguro para JSON"""
    row_dict = row.to_dict()
    for key, value in row_dict.items():
        if isinstance(value, pd.Timestamp):
            row_dict[key] = value.isoformat()
        elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            row_dict[key] = None
    return row_dict


def convert_row_to_json_safe_dict_exclude(row, exclude_fields=None):
    """Convierte una fila de DataFrame a un diccionario seguro para JSON, omitiendo columnas indicadas"""
    if exclude_fields is None:
        exclude_fields = []
    row_dict = row.to_dict()
    # Omitir columnas excluidas
    row_dict = {k: v for k, v in row_dict.items() if k not in exclude_fields}
    for key, value in row_dict.items():
        if isinstance(value, pd.Timestamp):
            row_dict[key] = value.isoformat()
        elif isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            row_dict[key] = None
    return row_dict

def ensure_output_dir(output_dir):
    """Crea el directorio de salida si no existe"""
    os.makedirs(output_dir, exist_ok=True)
    return output_dir