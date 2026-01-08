from pydantic import BaseModel, EmailStr
from typing import Optional


class LoginRequest(BaseModel):
    """
    Schema for login request
    """
    username: str
    password: str


class LoginResponse(BaseModel):
    """
    Schema for login response
    """
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class TokenData(BaseModel):
    """
    Schema for decoded token data
    """
    user_id: Optional[int] = None
    username: Optional[str] = None
    email: Optional[str] = None


class UserResponse(BaseModel):
    """
    Schema for user information response
    """
    id: int
    username: str
    email: str
    first_name: str
    last_name: str
    is_active: bool

    class Config:
        from_attributes = True
