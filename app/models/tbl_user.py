from datetime import datetime
from decimal import Decimal
from typing import Optional
from sqlmodel import Field, SQLModel, Column, Numeric
import sqlalchemy as sa


class TblUser(SQLModel, table=True):
    __tablename__ = "tbl_user"
    __table_args__ = {"schema": "sch_collectors"}

    id: Optional[int] = Field(default=None, primary_key=True)
    first_name: str = Field(...)
    last_name: str = Field(...)
    email: str = Field(...)
    password: str = Field(...)  # stored as hashed password
    profile_image: Optional[str] = Field(default=None)
    username: str = Field(...)
    is_active: bool = Field(default=True)
    channel_id: Optional[int] = Field(default=None)
    expiration_password: Optional[datetime] = Field(default=None)
    flag_password: bool = Field(default=False)
    dark_mode: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric))
    updated_by: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric))
    deleted_at: Optional[datetime] = Field(default=None)
