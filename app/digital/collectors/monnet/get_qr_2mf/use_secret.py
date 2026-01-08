# get_qr_2mf/use_secret.py

from app.digital.collectors.monnet.get_qr_2mf.storage import load_secret
from app.digital.collectors.monnet.get_qr_2mf.totp_generator import generate_otp

def main():
    print("[INFO] Generador de codigo OTP desde la clave guardada")
    label = "Monnet Payin:victor.olivares"
    secret = load_secret(label)
    if not secret:
        print(f"[!] No se encontro una clave guardada con el nombre '{label}'.")
        return

    code = generate_otp(secret)
    print(f"[✔] Codigo OTP para {label}: {code}")
    return code

if __name__ == "__main__":
    main()
