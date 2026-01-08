from pyzbar.pyzbar import decode
from PIL import Image

def extract_qr_data(image: Image.Image):
    qr_codes = decode(image)
    for qr in qr_codes:
        data = qr.data.decode('utf-8')
        if data.startswith("otpauth://"):
            return data
    return None
