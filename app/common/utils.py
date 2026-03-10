from datetime import datetime

def log_message(message: str) -> None:
    print(f"[LOG] {message}")


def handle_error(error: Exception) -> None:
    print(f"[ERROR] {str(error)}")



def validate_date_range(from_date, to_date):
    try:
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%d%m%y")
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%d%m%y")
    except Exception as e:
        print(f"[error] formato de fecha invalido en kashio: {e}")
        return False, None, None

    # validar rango maximo de 10 dias (conteo inclusivo)
    try:
        days_diff = (to_date.replace(tzinfo=None) - from_date.replace(tzinfo=None)).days + 1
        if days_diff > 10:
            print(f"[error] kashio: el rango solicitado ({days_diff} dias) excede el maximo de 10 dias")
            return False, None, None
    except Exception as e:
        print(f"[error] calculando el rango en kashio: {e}")
        return False, None, None
    
    return True, from_date, to_date
