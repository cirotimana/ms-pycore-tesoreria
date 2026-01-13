from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlmodel import Session, select
from typing import Optional
import logging

from app.auth.utils import decode_access_token
from app.auth.schemas import TokenData
from app.models.tbl_user import TblUser
from app.common.database import get_dts_session, get_dts_aws_session ##get_dts_aws_session

# configurar logger
logger = logging.getLogger(__name__)

# esquema de seguridad para bearer token
security = HTTPBearer(auto_error=True)


def get_current_user_from_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    session: Session = Depends(get_dts_aws_session)  ### para que valide con usuarios en aws
) -> TblUser:

    # obtener el token del header
    token = credentials.credentials
    logger.info(f"token recibido: {token[:20]}...")

    # decodificar el token
    token_data: Optional[TokenData] = decode_access_token(token)
    logger.info(f"token decodificado: user_id={token_data.user_id if token_data else None}")

    if token_data is None or token_data.user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="token invalido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # buscar el usuario en la base de datos
    statement = select(TblUser).where(
        TblUser.id == token_data.user_id,
        TblUser.is_active == True,
        TblUser.deleted_at.is_(None)
    )
    user = session.exec(statement).first()
    logger.info(f"usuario encontrado: {user.username if user else 'none'}")

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="usuario no encontrado o inactivo",
            headers={"WWW-Authenticate": "Bearer"},
        )

    logger.info(f"autenticacion exitosa para usuario: {user.username}")
    return user


def get_current_active_user(
    current_user: TblUser = Depends(get_current_user_from_token)
) -> TblUser:
    
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="usuario inactivo"
        )

    return current_user
