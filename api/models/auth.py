from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime
from api.types.roles import Role
from enum import Enum


class EncryptionKeyInfo(BaseModel):
    key: bytes
    expires_at: Optional[datetime] = None

    def is_expired(self, current_time: datetime) -> bool:
        if self.expires_at:
            expires = self.expires_at
            if expires.tzinfo is None:
                from datetime import timezone

                expires = expires.replace(tzinfo=timezone.utc)
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
            return current_time > expires
        return False


class EncryptionKeyDocument(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[str] = Field(None, alias="_id")
    symmetric_key: bytes
    is_active: bool = True
    expired_at: Optional[datetime] = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class UserDocument(BaseModel):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[str] = Field(None, alias="_id")
    email: str
    hashed_password: str
    role: Role = Role.USER
    is_active: bool = True
    created_at: datetime
    updated_at: Optional[datetime] = None


class ApiKeyDocument(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[str] = Field(None, alias="_id")
    name: str
    user_id: str
    key_id: str
    key_credential: bytes
    key_signature: bytes
    is_active: bool = True
    created_at: datetime
    updated_at: Optional[datetime] = None


class TokenPayload(BaseModel):
    user_id: str
    role: Role
    api_key_id: Optional[str] = None


class TokenData(TokenPayload):
    exp: Optional[int] = None
    iat: Optional[int] = None
    iss: Optional[str] = None
    aud: Optional[str] = None
    jti: Optional[str] = None


class ApiKeyCreateRequest(BaseModel):
    name: str = Field(
        ..., min_length=1, max_length=100, description="Name for the API key"
    )


class ApiKeyCreateResponse(BaseModel):
    id: str
    api_key: str
    name: str
    created_at: datetime


class ApiKeyListItem(BaseModel):
    id: str
    name: str
    created_at: datetime
    is_active: bool


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class LoginRequest(BaseModel):
    email: str = Field(..., description="User email address")
    password: str = Field(..., min_length=1, description="User password")


class RegisterUserRequest(BaseModel):
    email: str = Field(..., description="Email address for the new user")
    password: str = Field(..., min_length=6, description="Password (min 6 characters)")
    role: Role = Field(default=Role.USER, description="User role (USER, ADMIN, AGENT)")


class RegisterUserResponse(BaseModel):
    id: str
    email: str
    role: Role
    created_at: datetime


# Activity Log Models
class ActivityType(str, Enum):
    LOGIN = "LOGIN"
    LOGOUT = "LOGOUT"


class ActivityLogDocument(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[str] = Field(None, alias="_id")
    user_id: str
    user_email: str
    activity_type: ActivityType
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    success: bool = True
    details: Optional[str] = None
    created_at: datetime


class ActivityLogResponse(BaseModel):
    id: str
    user_id: str
    user_email: str
    activity_type: ActivityType
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    success: bool = True
    details: Optional[str] = None
    created_at: datetime


class ActivityLogListResponse(BaseModel):
    items: list[ActivityLogResponse]
    total: int
    page: int
    page_size: int
    total_pages: int
