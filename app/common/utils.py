def log_message(message: str) -> None:
    print(f"[LOG] {message}")


def handle_error(error: Exception) -> None:
    print(f"[ERROR] {str(error)}")
