# get_qr_2mf/main.py
from app.digital.collectors.monnet.get_qr_2mf.capture_screen import capture_full_screen
from app.digital.collectors.monnet.get_qr_2mf.detect_qr import extract_qr_data
from app.digital.collectors.monnet.get_qr_2mf.totp_generator import generate_otp, extract_secret_from_otpauth
from app.digital.collectors.monnet.get_qr_2mf.storage import save_secret

def main():
    print("[info] Capturando pantalla completa...")
    image = capture_full_screen()

    print("[info] Detectando QR en la imagen...")
    qr_data = extract_qr_data(image)
    if not qr_data:
        print("[alerta] No se detecto ningun codigo QR.")
        return

    print(f"[info] QR detectado: {qr_data}")
    secret, label = extract_secret_from_otpauth(qr_data)
    if not secret:
        print("[alerta] No se pudo extraer la clave del QR.")
        return

    print(f"[info] clave extraida para: {label}")
    save_secret(label, secret)
    print(f"[info] clave guardada con exito bajo el nombre '{label}'.")

    code = generate_otp(secret)
    print(f"[info] Codigo OTP actual para {label}: {code}")

if __name__ == "__main__":
    main()
