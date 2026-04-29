import structlog
from typing import Annotated, Optional
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import ExpiredSignatureError
from api.models.auth import Role, TokenData
from api.services.auth_service import (
    AuthService,
    InvalidTokenError,
    KeyNotFoundError,
    SigningKeyExpiredError,
)
from shared.database.connection import get_async_db


log = structlog.get_logger()

jwt_bearer = HTTPBearer(auto_error=False)


async def get_db():
    return get_async_db()


async def get_auth_service(db=Depends(get_db)):
    return AuthService(db=db)


DbDep = Annotated[object, Depends(get_db)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(jwt_bearer),
    auth_service: AuthService = Depends(get_auth_service),
) -> TokenData:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication. Provided a valid JWT Bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    try:
        token_data = await auth_service.verify_jwt_token(token)
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return token_data

    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={
                "WWW-Authenticate": 'Bearer error="invalid_token", error_description="token has expired"'
            },
        )
    except KeyNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Signing key not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except SigningKeyExpiredError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Signing key has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        log.exception("jwt.verification_error", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or malformed token",
            headers={"WWW-Authenticate": "Bearer"},
        )


CurrentUserDep = Annotated[TokenData, Depends(get_current_user)]


class RoleChecker:
    def __init__(self, allowed_roles: list[Role]):
        self.allowed_roles = allowed_roles

    def __call__(self, token_data: TokenData = Depends(get_current_user)) -> TokenData:
        if token_data.role not in self.allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions to perform this action",
            )
        return token_data


AdminOnly = Depends(RoleChecker([Role.ADMIN]))
UserOrAdmin = Depends(RoleChecker([Role.USER, Role.ADMIN]))
AllRoles = Depends(RoleChecker([Role.USER, Role.ADMIN, Role.AGENT]))

AdminUserDep = Annotated[TokenData, AdminOnly]
StandardUserDep = Annotated[TokenData, UserOrAdmin]
AnyUserDep = Annotated[TokenData, AllRoles]
