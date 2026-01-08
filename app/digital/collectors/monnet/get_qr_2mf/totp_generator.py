# get_qr_2mf/totp_generator.py
from urllib.parse import urlparse, parse_qs, unquote
import pyotp

def extract_secret_from_otpauth(uri):
    try:
        parsed = urlparse(uri)
        if parsed.scheme != "otpauth":
            return None, None
        secret = parse_qs(parsed.query).get("secret", [None])[0]
        label = unquote(parsed.path[1:])  # elimina la primera barra
        return secret, label
    except Exception:
        return None, None

def generate_otp(secret):
    totp = pyotp.TOTP(secret)
    return totp.now()
