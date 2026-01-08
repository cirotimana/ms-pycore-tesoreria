# get_qr_2mf/main.py
from app.digital.collectors.nuvei.get_qr_2mf.capture_screen import capture_full_screen
from app.digital.collectors.nuvei.get_qr_2mf.detect_qr import extract_qr_data
from app.digital.collectors.nuvei.get_qr_2mf.totp_generator import generate_otp, extract_secret_from_otpauth
from app.digital.collectors.nuvei.get_qr_2mf.storage import save_secret

def main():
    print("[+] Capturando pantalla completa...")
    image = capture_full_screen()

    print("[+] Detectando QR en la imagen...")
    qr_data = extract_qr_data(image)
    if not qr_data:
        print("[!] No se detecto ningun codigo QR.")
        return

    print(f"[+] QR detectado: {qr_data}")
    secret, label = extract_secret_from_otpauth(qr_data)
    if not secret:
        print("[!] No se pudo extraer la clave del QR.")
        return

    print(f"[+] clave extraida para: {label}")
    save_secret(label, secret)
    print(f"[+] clave guardada con exito bajo el nombre '{label}'.")

    code = generate_otp(secret)
    print(f"[+] Codigo OTP actual para {label}: {code}")

if __name__ == "__main__":
    main()
