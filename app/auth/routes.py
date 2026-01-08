from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlmodel import Session, select

from app.auth.schemas import LoginRequest, LoginResponse, UserResponse
from app.auth.utils import verify_password, create_access_token, get_token_expiration_seconds
from app.auth.dependencies import get_current_active_user
from app.models.user import User
from app.common.database import get_dts_session


router = APIRouter()


@router.get("/debug-headers")
def debug_headers(request: Request):
    """
    endpoint de debug para ver los headers recibidos
    """
    headers = dict(request.headers)
    return {
        "headers": headers,
        "has_authorization": "authorization" in headers,
        "authorization_value": headers.get("authorization", "no presente")
    }


@router.post("/login", response_model=LoginResponse)
def login(
    credentials: LoginRequest,
    session: Session = Depends(get_dts_session)
):
    # buscar usuario por username
    statement = select(User).where(
        User.username == credentials.username,
        User.is_active == True,
        User.deleted_at.is_(None)
    )
    user = session.exec(statement).first()

    # verificar si el usuario existe
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="credenciales invalidas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # verificar la contraseña
    if not verify_password(credentials.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="credenciales invalidas",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # crear el token jwt
    token_data = {
        "user_id": user.id,
        "username": user.username,
        "email": user.email
    }

    access_token = create_access_token(data=token_data)

    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=get_token_expiration_seconds()
    )


@router.get("/me", response_model=UserResponse, dependencies=[Depends(get_current_active_user)])
def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    return UserResponse(
        id=current_user.id,
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        is_active=current_user.is_active
    )
