from datetime import datetime

def log_message(message: str) -> None:
    print(f"[log] {message}")


def handle_error(error: Exception) -> None:
    print(f"[error] {str(error)}")


def validate_date_range(from_date, to_date):
    try:
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%d%m%y")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%d%m%y")
    except Exception as e:
        print(f"[error] formato de fecha invalido: {e}")
        return False, None, None

    # validar rango maximo de 10 dias (conteo inclusivo)
    try:
        days_diff = (to_date.replace(tzinfo=None) - from_date.replace(tzinfo=None)).days + 1
        if days_diff > 10:
            print(f"[error] el rango solicitado ({days_diff} dias) excede el maximo de 10 dias")
            return False, None, None
    except Exception as e:
        print(f"[error] calculando el rango de fechas: {e}")
        return False, None, None
    
    return True, from_date, to_date


def format_duration(seconds: float) -> str:
    # convierte segundos a formato M:S (ej: 4:37 minutos)
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    return f"{minutes}:{remaining_seconds:02d}"
