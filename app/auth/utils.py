
import jwt
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from app.config import Config
from app.auth.schemas import TokenData

from passlib.context import CryptContext

# constantes para jwt
JWT_EXPIRATION_DAYS = 1  # token expira en 1 dia
JWT_EXPIRATION_SECONDS = JWT_EXPIRATION_DAYS * 24 * 60 * 60  # 86400 segundos

# configurar contexto de passlib para bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


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
    print(f"[debug] ====== iniciando decodificacion de token ======")
    print(f"[debug] token recibido (primeros 50 chars): {token[:50]}")
    print(f"[debug] secret_key: {Config.JWT_SECRET_KEY}")
    print(f"[debug] algoritmo: {Config.JWT_ALGORITHM}")
    
    try:
        payload = jwt.decode(
            token,
            Config.JWT_SECRET_KEY,
            algorithms=[Config.JWT_ALGORITHM],
            options={"verify_sub": False}  # Desactivar validación de 'sub' si no se envía
        )

        print(f"[ok] token decodificado exitosamente")
        print(f"[debug] payload completo: {payload}")
        print(f"[debug] claves en el payload: {list(payload.keys())}")

        # PyJWT normaliza 'user_id' a 'sub' automáticamente
        # Intentar obtener del claim 'sub' primero (normalizado), luego de 'user_id'
        user_id: int = payload.get("sub") or payload.get("user_id")
        username: str = payload.get("username")
        email: str = payload.get("email")

        print(f"[debug] valores extraidos -> user_id={user_id}, username={username}, email={email}")

        if user_id is None:
            print("[warn] user_id es none en el payload del token")
            return None

        print(f"[debug] token valido para user_id={user_id}")
        return TokenData(user_id=user_id, username=username, email=email)

    except jwt.ExpiredSignatureError as e:
        print(f"[error] token ha expirado: {e}")
        return None
    except jwt.DecodeError as e:
        print(f"[error] error decodificando token: {e}")
        return None
    except jwt.InvalidSignatureError as e:
        print(f"[error] firma de token invalida: {e}")
        return None
    except Exception as e:
        print(f"[error] error inesperado decodificando token: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_token_expiration_seconds() -> int:
    return JWT_EXPIRATION_SECONDS
