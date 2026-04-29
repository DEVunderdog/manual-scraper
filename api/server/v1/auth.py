import structlog
from fastapi import APIRouter, Depends, HTTPException, status, Header, Request
from api.models.auth import (
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyListItem,
    TokenResponse,
    LoginRequest,
    RegisterUserRequest,
    RegisterUserResponse,
    ActivityType,
)
from api.server.dependencies import (
    AuthServiceDep,
    CurrentUserDep,
    get_auth_service,
    AdminUserDep,
)
from api.services.auth_service import (
    AuthService,
    KeyNotFoundError,
    SigningKeyExpiredError,
)
from shared.config.settings import get_settings
from api.types.roles import Role

log = structlog.get_logger()
router = APIRouter(prefix="/auth", tags=["Authentication"])


def get_client_ip(request: Request) -> str:
    """Extract client IP from request headers or connection."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_user_agent(request: Request) -> str:
    """Extract user agent from request headers."""
    return request.headers.get("User-Agent", "unknown")


@router.post(
    "/login",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
)
async def login(
    request: LoginRequest,
    http_request: Request,
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Login with email and password to get a JWT token.

    **Request Body:**
    - `email`: User email address
    - `password`: User password

    **Returns:**
    - `access_token`: JWT token to use for authentication
    - `token_type`: "bearer"
    - `expires_in`: Token validity in seconds
    """
    ip_address = get_client_ip(http_request)
    user_agent = get_user_agent(http_request)

    try:
        user = await auth_service.authenticate_user(request.email, request.password)

        if user is None:
            # Log failed login attempt
            await auth_service.log_activity(
                user_id="unknown",
                user_email=request.email,
                activity_type=ActivityType.LOGIN,
                ip_address=ip_address,
                user_agent=user_agent,
                success=False,
                details="Invalid email or password",
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )

        token, expires_in = await auth_service.create_jwt_token_for_user(
            user_id=user.id,
            role=user.role,
        )

        # Log successful login
        await auth_service.log_activity(
            user_id=user.id,
            user_email=user.email,
            activity_type=ActivityType.LOGIN,
            ip_address=ip_address,
            user_agent=user_agent,
            success=True,
        )

        log.info(
            "user.login",
            user_id=user.id,
            email=user.email,
        )

        return TokenResponse(access_token=token, expires_in=expires_in)

    except SigningKeyExpiredError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key has expired. Contact administrator.",
        )
    except KeyNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key not found. Contact administrator.",
        )


@router.post(
    "/logout",
    status_code=status.HTTP_200_OK,
)
async def logout(
    http_request: Request,
    auth_service: AuthServiceDep,
    current_user: CurrentUserDep,
):
    """
    Logout the current user. **Requires JWT Bearer token.**

    Logs the logout activity. Client should discard the JWT token.

    **Returns:**
    - Success message
    """
    ip_address = get_client_ip(http_request)
    user_agent = get_user_agent(http_request)

    # Get user email for logging
    user = await auth_service.get_user_by_id(current_user.user_id)
    user_email = user.email if user else "unknown"

    # Log logout activity
    await auth_service.log_activity(
        user_id=current_user.user_id,
        user_email=user_email,
        activity_type=ActivityType.LOGOUT,
        ip_address=ip_address,
        user_agent=user_agent,
        success=True,
    )

    log.info(
        "user.logout",
        user_id=current_user.user_id,
        email=user_email,
    )

    return {"message": "Logged out successfully"}


@router.post(
    "/register",
    response_model=RegisterUserResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_user(
    request: RegisterUserRequest,
    auth_service: AuthServiceDep,
    current_user: AdminUserDep,
):
    """
    Register a new user. **Admin only.**

    Only administrators can create new user accounts.

    **Request Body:**
    - `email`: Email address for the new user
    - `password`: Password (min 6 characters)
    - `role`: User role (USER, ADMIN, AGENT) - defaults to USER

    **Returns:**
    - User details (id, email, role, created_at)
    """
    # Check if email already exists
    existing_user = await auth_service.get_user_by_email(request.email.lower().strip())
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    try:
        user = await auth_service.create_user(
            email=request.email.lower().strip(),
            password=request.password,
            role=request.role,
        )

        log.info(
            "user.registered",
            user_id=user.id,
            email=user.email,
            role=user.role,
            registered_by=current_user.user_id,
        )

        return RegisterUserResponse(
            id=user.id,
            email=user.email,
            role=user.role,
            created_at=user.created_at,
        )

    except Exception as e:
        log.exception("user.registration_failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to register user",
        )


@router.post(
    "/token",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
)
async def get_token(
    x_api_key: str = Header(None, alias="X-API-Key"),
    auth_service: AuthService = Depends(get_auth_service),
):
    """
    Exchange an API key for a JWT token.

    This is the ONLY endpoint that accepts API key authentication.
    All other endpoints require JWT Bearer token.

    **Usage:**
    1. Use this endpoint with your API key to get a JWT token
    2. Use the JWT token for all other API calls

    **Request Body:**
    - `api_key`: Your API key (format: sk-xxx.yyy)

    **Returns:**
    - `access_token`: JWT token to use for authentication
    - `token_type`: "bearer"
    - `expires_in`: Token validity in seconds
    """
    try:
        if x_api_key is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid API key"
            )

        result = await auth_service.validate_api_key(x_api_key)

        if result is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
            )

        api_key_doc, user_doc = result

        token, expires_in = await auth_service.create_jwt_token(
            user_id=user_doc.id,
            role=user_doc.role,
            api_key_id=api_key_doc.id,
        )

        log.info(
            "token.issued",
            user_id=user_doc.id,
            api_key_id=api_key_doc.id,
        )

        return TokenResponse(access_token=token, expires_in=expires_in)

    except SigningKeyExpiredError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key has expired. Contact administrator.",
        )
    except KeyNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key not found. Contact administrator.",
        )


@router.post(
    "/keys",
    response_model=ApiKeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_api_key(
    request: ApiKeyCreateRequest,
    auth_service: AuthServiceDep,
    current_user: CurrentUserDep,
):
    """
    Create a new API key. **Requires JWT Bearer token.**

    The API key will be shown only once at creation time.
    Store it securely as it cannot be retrieved later.

    **Request Body:**
    - `name`: A descriptive name for the API key

    **Returns:**
    - `id`: API key ID
    - `api_key`: The actual API key (SAVE THIS - shown only once)
    - `name`: The name you provided
    - `created_at`: Creation timestamp
    """
    try:
        raw_key, doc = await auth_service.create_api_key(
            name=request.name,
            user_id=current_user.user_id,
        )

        log.info(
            "api_key.created",
            api_key_id=doc.id,
            user_id=current_user.user_id,
            name=request.name,
        )

        return ApiKeyCreateResponse(
            id=doc.id,
            api_key=raw_key,
            name=doc.name,
            created_at=doc.created_at,
        )

    except SigningKeyExpiredError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key has expired. Contact administrator.",
        )
    except KeyNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Signing key not found. Contact administrator.",
        )


@router.get(
    "/keys",
    response_model=list[ApiKeyListItem],
    status_code=status.HTTP_200_OK,
)
async def list_api_keys(
    auth_service: AuthServiceDep,
    current_user: CurrentUserDep,
):
    """
    List all active API keys for the current user. **Requires JWT Bearer token.**

    Note: The actual API key values are not returned for security.
    Only metadata (id, name, created_at, is_active) is shown.

    **Returns:**
    - List of API key metadata objects
    """
    keys = await auth_service.list_api_keys(current_user.user_id)
    return [
        ApiKeyListItem(
            id=k.id,
            name=k.name,
            created_at=k.created_at,
            is_active=k.is_active,
        )
        for k in keys
    ]


@router.delete(
    "/keys/{api_key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_api_key(
    api_key_id: str,
    auth_service: AuthServiceDep,
    current_user: CurrentUserDep,
):
    """
    Deactivate an API key. **Requires JWT Bearer token.**

    This will invalidate the API key, preventing it from being used
    to generate new JWT tokens. Existing JWT tokens will remain valid
    until they expire.

    **Path Parameters:**
    - `api_key_id`: The ID of the API key to deactivate
    """
    success = await auth_service.deactivate_api_key(
        api_key_id=api_key_id,
        user_id=current_user.user_id,
    )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="API key not found or already deactivated",
        )

    log.info(
        "api_key.deactivated",
        api_key_id=api_key_id,
        user_id=current_user.user_id,
    )


@router.get(
    "/me",
    status_code=status.HTTP_200_OK,
)
async def get_current_user_info(
    current_user: CurrentUserDep,
    auth_service: AuthServiceDep,
):
    """
    Get information about the current authenticated user. **Requires JWT Bearer token.**

    **Returns:**
    - `user_id`: User ID
    - `role`: User role (USER, ADMIN, AGENT)
    - `api_key_id`: The API key ID used to generate the token
    """
    user = await auth_service.get_user_by_id(current_user.user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return {
        "user_id": user.id,
        "email": user.email,
        "role": user.role,
        "api_key_id": current_user.api_key_id,
    }


@router.get(
    "/default-key",
    status_code=status.HTTP_200_OK,
)
async def get_default_api_key(
    auth_service: AuthServiceDep,
):
    """
    Get the default API key for testing/bootstrap purposes.

    **Note:** This endpoint is for development/testing only.
    In production, disable or protect this endpoint.

    **Returns:**
    - `api_key`: The default API key
    """

    _settings = get_settings()

    default_key = None

    if _settings.is_development:
        default_key = await auth_service.get_default_api_key()

    if not default_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Default API key not found",
        )

    return {"api_key": default_key}
