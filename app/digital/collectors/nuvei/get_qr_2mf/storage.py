import json
import os
from pathlib import Path

# Ruta real hasta la raiz del proyecto (usa .git como ancla si existe, o el path del script raiz)
def find_project_root():
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / ".git").exists() or (current / "secrets.json").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parents[3]  # fallback

PROJECT_ROOT = find_project_root()
SECRET_FILE = PROJECT_ROOT / "secrets.json"

def save_secret(label, secret):
    secrets = {}
    if SECRET_FILE.exists():
        with open(SECRET_FILE, "r") as f:
            secrets = json.load(f)
    secrets[label] = secret
    with open(SECRET_FILE, "w") as f:
        json.dump(secrets, f, indent=2)
    print(f"[✔] Clave guardada en: {SECRET_FILE}")

def load_secret(label):
    if not SECRET_FILE.exists():
        print(f"[!] No se encontro el archivo: {SECRET_FILE}")
        return None
    with open(SECRET_FILE, "r") as f:
        secrets = json.load(f)
    secret = secrets.get(label)
    if secret:
        print(f"[✔] Clave recuperada para '{label}' desde: {SECRET_FILE}")
    else:
        print(f"[!] No se encontro la clave para '{label}' en: {SECRET_FILE}")
    return secret
