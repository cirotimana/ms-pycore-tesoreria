import hashlib
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from app.config import Config
from app.auth.schemas import TokenData


# constantes para jwt
JWT_EXPIRATION_DAYS = 1  # token expira en 1 dia
JWT_EXPIRATION_SECONDS = JWT_EXPIRATION_DAYS * 24 * 60 * 60  # 86400 segundos


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(seconds=JWT_EXPIRATION_SECONDS)

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow()
    })

    encoded_jwt = jwt.encode(
        to_encode,
        Config.JWT_SECRET_KEY,
        algorithm=Config.JWT_ALGORITHM
    )

    return encoded_jwt


def decode_access_token(token: str) -> Optional[TokenData]:
    print(f"[DEBUG] ====== INICIANDO DECODIFICACION DE TOKEN ======")
    print(f"[DEBUG] Token recibido (primeros 50 chars): {token[:50]}")
    print(f"[DEBUG] SECRET_KEY: {Config.JWT_SECRET_KEY}")
    print(f"[DEBUG] Algoritmo: {Config.JWT_ALGORITHM}")
    
    try:
        payload = jwt.decode(
            token,
            Config.JWT_SECRET_KEY,
            algorithms=[Config.JWT_ALGORITHM],
            options={"verify_sub": False}  # Desactivar validación de 'sub' si no se envía
        )

        print(f"[DEBUG] ✓ Token decodificado exitosamente")
        print(f"[DEBUG] Payload COMPLETO: {payload}")
        print(f"[DEBUG] Claves en el payload: {list(payload.keys())}")

        # PyJWT normaliza 'user_id' a 'sub' automáticamente
        # Intentar obtener del claim 'sub' primero (normalizado), luego de 'user_id'
        user_id: int = payload.get("sub") or payload.get("user_id")
        username: str = payload.get("username")
        email: str = payload.get("email")

        print(f"[DEBUG] Valores extraídos -> user_id={user_id}, username={username}, email={email}")

        if user_id is None:
            print("[WARN] ⚠ user_id es None en el payload del token")
            return None

        print(f"[DEBUG] ✓ Token válido para user_id={user_id}")
        return TokenData(user_id=user_id, username=username, email=email)

    except jwt.ExpiredSignatureError as e:
        print(f"[ERROR] ✗ Token ha expirado: {e}")
        return None
    except jwt.DecodeError as e:
        print(f"[ERROR] ✗ Error decodificando token: {e}")
        return None
    except jwt.InvalidSignatureError as e:
        print(f"[ERROR] ✗ Firma de token inválida: {e}")
        return None
    except Exception as e:
        print(f"[ERROR] ✗ Error inesperado decodificando token: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_token_expiration_seconds() -> int:
    return JWT_EXPIRATION_SECONDS
