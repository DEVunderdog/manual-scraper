import base64
import hashlib
import hmac
import secrets
import uuid
import structlog
from bson import ObjectId
from jose import jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from api.models.auth import (
    EncryptionKeyInfo,
    EncryptionKeyDocument,
    UserDocument,
    ApiKeyDocument,
    TokenData,
    ActivityType,
    ActivityLogDocument,
)
from api.constants.globals import (
    ENCRYPTION_KEY_EXPIRY_DAYS,
    JWT_TOKEN_EXPIRATION_MINUTES,
    JWT_ISSUER,
    JWT_AUDIENCE,
    JWT_ALGORITHM,
)
from api.types.roles import Role

log = structlog.get_logger()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class KeyNotFoundError(Exception):
    pass


class SigningKeyExpiredError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class InvalidApiKeyError(Exception):
    pass


class AuthService:
    def __init__(self, db):
        self._db = db
        self._encryption_keys = db.encryption_keys
        self._api_keys = db.api_keys
        self._users = db.users
        self._activity_logs = db.activity_logs
        self._active_key_cache: Optional[Tuple[Dict[str, EncryptionKeyInfo], str]] = (
            None
        )

    async def _generate_symmetric_key(self) -> bytes:
        return secrets.token_bytes(32)

    async def _create_encryption_key(self) -> EncryptionKeyDocument:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=ENCRYPTION_KEY_EXPIRY_DAYS)

        symmetric_key = await self._generate_symmetric_key()

        doc = {
            "symmetric_key": symmetric_key,
            "is_active": True,
            "expired_at": expires_at,
            "created_at": now,
            "updated_at": None,
        }

        result = await self._encryption_keys.insert_one(doc)

        doc["_id"] = str(result.inserted_id)

        log.info("encryption_key.created", key_id=doc["_id"])

        return EncryptionKeyDocument(**doc)

    async def _get_active_encryption_key(self) -> Optional[EncryptionKeyDocument]:
        doc = await self._encryption_keys.find_one(
            {"is_active": True},
        )
        if doc:
            doc["_id"] = str(doc["_id"])
            return EncryptionKeyDocument(**doc)
        return None

    async def _mark_all_keys_inactive(self) -> None:
        await self._encryption_keys.update_many(
            {"is_active": True},
            {"$set": {"is_active": False, "updated_at": datetime.now(timezone.utc)}},
        )
        log.info("encryption_keys.all_marked_inactive")

    async def ensure_active_key(self, rotate: bool) -> None:
        """Ensure there's an active encryption key, create if needed."""
        if rotate:
            await self._mark_all_keys_inactive()
            await self._create_encryption_key()
            log.info("encryption_key.rotated")
        else:
            active_key = await self._get_active_encryption_key()
            if active_key is None:
                await self._create_encryption_key()
                log.info("encryption_key.created_initial")

    async def _build_key_cache(self) -> Tuple[Dict[str, EncryptionKeyInfo], str]:
        active_key = await self._get_active_encryption_key()

        if active_key is None:
            raise KeyNotFoundError("No active encryption key found")

        key_info: Dict[str, EncryptionKeyInfo] = {}
        active_id = active_key.id

        key_info[active_id] = EncryptionKeyInfo(
            key=active_key.symmetric_key,
            expires_at=active_key.expired_at,
        )

        self._active_key_cache = (key_info, active_id)
        return self._active_key_cache

    async def _get_keys(self) -> Tuple[Dict[str, EncryptionKeyInfo], str]:
        if self._active_key_cache is None:
            return await self._build_key_cache()
        return self._active_key_cache

    async def create_user(
        self,
        email: str,
        password: str,
        role: Role = Role.USER,
    ) -> UserDocument:

        now = datetime.now(timezone.utc)
        hashed_password = pwd_context.hash(password)

        doc = {
            "email": email,
            "hashed_password": hashed_password,
            "role": role,
            "is_active": True,
            "created_at": now,
            "updated_at": None,
        }

        result = await self._users.insert_one(doc)
        doc["_id"] = str(result.inserted_id)

        log.info("user.created", user_id=doc["_id"], email=email)
        return UserDocument(**doc)

    async def get_user_by_id(self, user_id: str) -> Optional[UserDocument]:
        doc = await self._users.find_one({"_id": ObjectId(user_id), "is_active": True})
        if doc:
            doc["_id"] = str(doc["_id"])
            return UserDocument(**doc)
        return None

    async def get_user_by_email(self, email: str) -> Optional[UserDocument]:
        """Get user by email."""
        doc = await self._users.find_one({"email": email, "is_active": True})
        if doc:
            doc["_id"] = str(doc["_id"])
            return UserDocument(**doc)
        return None

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return pwd_context.verify(plain_password, hashed_password)

    async def authenticate_user(
        self, email: str, password: str
    ) -> Optional[UserDocument]:
        """Authenticate user by email and password."""
        user = await self.get_user_by_email(email.lower().strip())
        if not user:
            return None
        if not self.verify_password(password, user.hashed_password):
            return None
        return user

    async def create_jwt_token_for_user(
        self,
        user_id: str,
        role: Role,
    ) -> Tuple[str, int]:
        """Create JWT token for email/password login (no api_key_id)."""
        all_keys, active_key_id = await self._get_keys()

        if active_key_id not in all_keys:
            raise KeyNotFoundError("Active encryption key not found")

        active_key_info = all_keys[active_key_id]
        now = datetime.now(timezone.utc)

        if active_key_info.is_expired(now):
            raise SigningKeyExpiredError("Active signing key has expired")

        expires_delta = timedelta(minutes=JWT_TOKEN_EXPIRATION_MINUTES)
        expire = now + expires_delta

        payload = {
            "user_id": user_id,
            "role": role,
            "api_key_id": None,  # No API key for email/password login
            "exp": int(expire.timestamp()),
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "iss": JWT_ISSUER,
            "aud": JWT_AUDIENCE,
            "jti": str(uuid.uuid4()),
        }

        headers = {"kid": active_key_id}

        token = jwt.encode(
            payload,
            active_key_info.key,
            algorithm=JWT_ALGORITHM,
            headers=headers,
        )

        return token, JWT_TOKEN_EXPIRATION_MINUTES * 60

    async def ensure_default_user(
        self,
        default_email: str,
        default_password: str,
    ) -> UserDocument:
        """
        Ensure default admin user exists with correct credentials.

        - If no default admin exists, create one
        - If default admin exists but credentials changed, delete old and create new
        - Always ensures admin privileges
        """
        # Get stored default admin info from settings
        stored_settings = await self._db.settings.find_one(
            {"key": "default_admin_email"}
        )
        stored_email = stored_settings.get("value") if stored_settings else None

        # Check if credentials have changed
        if stored_email and stored_email != default_email:
            # Email changed - delete old default admin and their API keys
            old_user_doc = await self._users.find_one({"email": stored_email})
            if old_user_doc:
                old_user_id = str(old_user_doc["_id"])
                log.info(
                    "default_user.credentials_changed",
                    old_email=stored_email,
                    new_email=default_email,
                )
                # Deactivate old user's API keys
                await self._api_keys.update_many(
                    {"user_id": old_user_id},
                    {
                        "$set": {
                            "is_active": False,
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )
                # Delete old user completely to avoid duplicate key issues
                await self._users.delete_one({"_id": old_user_doc["_id"]})
                log.info("default_user.old_admin_deleted", email=stored_email)

        # Check if user with new email exists (active or inactive)
        existing_doc = await self._users.find_one({"email": default_email})

        if existing_doc:
            existing_id = str(existing_doc["_id"])
            needs_update = False
            update_fields = {"updated_at": datetime.now(timezone.utc)}

            # Check if user is inactive - reactivate
            if not existing_doc.get("is_active", True):
                update_fields["is_active"] = True
                needs_update = True
                log.info("default_user.reactivating", email=default_email)

            # Check if password needs update
            if not self.verify_password(
                default_password, existing_doc["hashed_password"]
            ):
                update_fields["hashed_password"] = pwd_context.hash(default_password)
                needs_update = True
                log.info("default_user.password_updated", email=default_email)

            # Ensure admin role
            if existing_doc.get("role") != Role.ADMIN:
                update_fields["role"] = Role.ADMIN
                needs_update = True
                log.info("default_user.role_upgraded_to_admin", email=default_email)

            if needs_update:
                await self._users.update_one(
                    {"_id": existing_doc["_id"]}, {"$set": update_fields}
                )

            # Fetch updated document
            updated_doc = await self._users.find_one({"_id": existing_doc["_id"]})
            updated_doc["_id"] = str(updated_doc["_id"])
            user = UserDocument(**updated_doc)
        else:
            # Create new default admin
            user = await self.create_user(
                email=default_email,
                password=default_password,
                role=Role.ADMIN,
            )
            log.info("default_user.created", email=default_email, role=Role.ADMIN)

        # Store current default admin email in settings
        await self._db.settings.update_one(
            {"key": "default_admin_email"},
            {
                "$set": {
                    "value": default_email,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )

        return user

    async def create_api_key(
        self, name: str, user_id: str
    ) -> Tuple[str, ApiKeyDocument]:
        all_keys, active_key_id = await self._get_keys()

        if active_key_id not in all_keys:
            raise KeyNotFoundError("Active encryption key not found")

        active_key_info = all_keys[active_key_id]
        now = datetime.now(timezone.utc)

        if active_key_info.is_expired(now):
            raise SigningKeyExpiredError("Active signing key has expired")

        # Generate random bytes for the API key
        random_bytes = secrets.token_bytes(24)
        random_bytes_b64 = base64.urlsafe_b64encode(random_bytes).decode("utf-8")

        # Create HMAC signature
        data_to_hmac = f"{active_key_id}:{random_bytes_b64}".encode("utf-8")
        hmac_obj = hmac.new(active_key_info.key, data_to_hmac, hashlib.sha256)
        signature_bytes = hmac_obj.digest()
        signature_b64 = base64.urlsafe_b64encode(signature_bytes).decode("utf-8")

        # Format: sk-{random}.{signature}
        api_key = f"sk-{random_bytes_b64}.{signature_b64}"
        api_key_bytes = api_key.encode("utf-8")

        doc = {
            "name": name,
            "user_id": user_id,
            "key_id": active_key_id,
            "key_credential": api_key_bytes,
            "key_signature": signature_bytes,
            "is_active": True,
            "created_at": now,
            "updated_at": None,
        }

        result = await self._api_keys.insert_one(doc)
        doc["_id"] = str(result.inserted_id)

        log.info("api_key.created", api_key_id=doc["_id"], user_id=user_id)
        return api_key, ApiKeyDocument(**doc)

    async def validate_api_key(
        self, raw_key: str
    ) -> Optional[Tuple[ApiKeyDocument, UserDocument]]:
        if raw_key is None:
            return None

        if not raw_key.startswith("sk-"):
            return None

        key_part = raw_key[3:]
        parts = key_part.split(".")
        if len(parts) != 2:
            return None

        random_bytes_b64, signature_b64 = parts
        api_key_bytes = raw_key.encode("utf-8")

        doc = await self._api_keys.find_one(
            {
                "key_credential": api_key_bytes,
                "is_active": True,
            }
        )

        if not doc:
            return None

        doc["_id"] = str(doc["_id"])
        api_key_doc = ApiKeyDocument(**doc)

        all_keys, _ = await self._get_keys()
        key_info = all_keys.get(api_key_doc.key_id)

        if not key_info:
            log.warning("api_key.validation_failed", reason="encryption_key_not_found")
            return None

        now = datetime.now(timezone.utc)
        if key_info.is_expired(now):
            raise SigningKeyExpiredError(
                f"Encryption key {api_key_doc.key_id} has expired"
            )

        # Verify HMAC signature
        data_to_hmac = f"{api_key_doc.key_id}:{random_bytes_b64}".encode("utf-8")
        expected_hmac = hmac.new(key_info.key, data_to_hmac, hashlib.sha256)
        expected_signature = expected_hmac.digest()

        try:
            client_signature = base64.urlsafe_b64decode(signature_b64)
        except Exception:
            log.warning("api_key.validation_failed", reason="invalid_signature_format")
            return None

        if not hmac.compare_digest(expected_signature, client_signature):
            log.warning("api_key.validation_failed", reason="signature_mismatch")
            return None

        # Get the associated user
        user_doc = await self.get_user_by_id(api_key_doc.user_id)
        if not user_doc:
            log.warning("api_key.validation_failed", reason="user_not_found")
            return None

        return api_key_doc, user_doc

    async def list_api_keys(self, user_id: str) -> list[ApiKeyDocument]:
        cursor = self._api_keys.find(
            {"user_id": user_id, "is_active": True},
        )
        docs = await cursor.to_list(length=100)
        result = []
        for d in docs:
            d["_id"] = str(d["_id"])
            result.append(ApiKeyDocument(**d))
        return result

    async def deactivate_api_key(self, api_key_id: str, user_id: str) -> bool:
        try:
            if not ObjectId.is_valid(api_key_id):
                return False
            result = await self._api_keys.update_one(
                {"_id": ObjectId(api_key_id), "user_id": user_id},
                {
                    "$set": {
                        "is_active": False,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )
            return result.modified_count > 0
        except Exception:
            return False

    async def create_jwt_token(
        self,
        user_id: str,
        role: Role,
        api_key_id: str,
    ) -> Tuple[str, int]:
        all_keys, active_key_id = await self._get_keys()

        if active_key_id not in all_keys:
            raise KeyNotFoundError("Active encryption key not found")

        active_key_info = all_keys[active_key_id]
        now = datetime.now(timezone.utc)

        if active_key_info.is_expired(now):
            raise SigningKeyExpiredError("Active signing key has expired")

        expires_delta = timedelta(minutes=JWT_TOKEN_EXPIRATION_MINUTES)
        expire = now + expires_delta

        payload = {
            "user_id": user_id,
            "role": role,
            "api_key_id": api_key_id,
            "exp": int(expire.timestamp()),
            "iat": int(now.timestamp()),
            "nbf": int(now.timestamp()),
            "iss": JWT_ISSUER,
            "aud": JWT_AUDIENCE,
            "jti": str(uuid.uuid4()),
        }

        # Include key ID in header for key rotation support
        headers = {"kid": active_key_id}

        token = jwt.encode(
            payload,
            active_key_info.key,
            algorithm=JWT_ALGORITHM,
            headers=headers,
        )

        return token, JWT_TOKEN_EXPIRATION_MINUTES * 60

    async def verify_jwt_token(self, token: str) -> Optional[TokenData]:
        try:
            unverified_headers = jwt.get_unverified_header(token)
            kid = unverified_headers.get("kid")

            if not kid:
                raise InvalidTokenError("token missing key ID in header")

            all_keys, _ = await self._get_keys()
            key_info = all_keys.get(kid)

            if not key_info:
                raise KeyNotFoundError(f"Key ID {kid} not found")

            now = datetime.now(timezone.utc)
            if key_info.is_expired(now):
                raise SigningKeyExpiredError(f"Signing key {kid} has expired")

            payload = jwt.decode(
                token,
                key_info.key,
                algorithms=[JWT_ALGORITHM],
                audience=JWT_AUDIENCE,
                issuer=JWT_ISSUER,
                options={
                    "verify_aud": True,
                    "verify_iss": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                },
            )

            return TokenData(**payload)

        except jwt.ExpiredSignatureError:
            log.warning("jwt.expired")
            raise
        except jwt.InvalidAudienceError:
            log.warning("jwt.invalid_audience")
            raise
        except jwt.InvalidIssuerError:
            log.warning("jwt.invalid_issuer")
            raise
        except jwt.JWTError as e:
            log.warning("jwt.invalid", error=str(e))
            raise
        except Exception as e:
            log.exception("jwt.verification_error", error=str(e))
            raise

    async def ensure_default_key(self) -> None:
        await self.ensure_active_key()

        default_user = await self.ensure_default_user()

        existing_keys = await self.list_api_keys(default_user.id)
        if not existing_keys:
            raw_key, _ = await self.create_api_key(
                name="Default API Key",
                user_id=default_user.id,
            )
            log.info(
                "default_api_key.created", hint="Use this key to generate JWT tokens"
            )

            await self._db.settings.update_one(
                {"key": "default_api_key"},
                {"$set": {"value": raw_key, "updated_at": datetime.now(timezone.utc)}},
                upsert=True,
            )

    async def get_default_api_key(self) -> Optional[str]:
        doc = await self._db.settings.find_one({"key": "default_api_key"})
        return doc.get("value") if doc else None

    # Activity Logging Methods
    async def log_activity(
        self,
        user_id: str,
        user_email: str,
        activity_type: ActivityType,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        success: bool = True,
        details: Optional[str] = None,
    ) -> ActivityLogDocument:
        """Log a user activity."""
        now = datetime.now(timezone.utc)

        doc = {
            "user_id": user_id,
            "user_email": user_email,
            "activity_type": activity_type,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "success": success,
            "details": details,
            "created_at": now,
        }

        result = await self._activity_logs.insert_one(doc)
        doc["_id"] = str(result.inserted_id)

        log.info(
            "activity.logged",
            activity_type=activity_type,
            user_email=user_email,
            success=success,
        )

        return ActivityLogDocument(**doc)

    async def get_activity_logs(
        self,
        page: int = 1,
        page_size: int = 50,
        user_email: Optional[str] = None,
        activity_type: Optional[ActivityType] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Tuple[list[ActivityLogDocument], int]:
        """Get activity logs with filtering and pagination."""
        query = {}

        if user_email:
            query["user_email"] = {"$regex": user_email, "$options": "i"}

        if activity_type:
            query["activity_type"] = activity_type

        if start_date or end_date:
            query["created_at"] = {}
            if start_date:
                query["created_at"]["$gte"] = start_date
            if end_date:
                query["created_at"]["$lte"] = end_date

        # Get total count
        total = await self._activity_logs.count_documents(query)

        # Get paginated results
        skip = (page - 1) * page_size
        cursor = (
            self._activity_logs.find(query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(page_size)
        )
        docs = await cursor.to_list(length=page_size)

        result = []
        for d in docs:
            d["_id"] = str(d["_id"])
            result.append(ActivityLogDocument(**d))

        return result, total

    async def get_activity_stats(self) -> dict:
        """Get activity statistics for dashboard."""
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=7)

        # Today's stats
        today_logins = await self._activity_logs.count_documents(
            {
                "activity_type": ActivityType.LOGIN,
                "success": True,
                "created_at": {"$gte": today_start},
            }
        )

        today_failed_logins = await self._activity_logs.count_documents(
            {
                "activity_type": ActivityType.LOGIN,
                "success": False,
                "created_at": {"$gte": today_start},
            }
        )

        # Week stats
        week_logins = await self._activity_logs.count_documents(
            {
                "activity_type": ActivityType.LOGIN,
                "success": True,
                "created_at": {"$gte": week_start},
            }
        )

        # Total unique users who logged in today
        pipeline = [
            {
                "$match": {
                    "activity_type": ActivityType.LOGIN,
                    "success": True,
                    "created_at": {"$gte": today_start},
                }
            },
            {"$group": {"_id": "$user_email"}},
            {"$count": "count"},
        ]
        unique_users_cursor = await self._activity_logs.aggregate(pipeline)
        unique_users_result = await unique_users_cursor.to_list(length=1)
        unique_users_today = (
            unique_users_result[0]["count"] if unique_users_result else 0
        )

        return {
            "today_logins": today_logins,
            "today_failed_logins": today_failed_logins,
            "week_logins": week_logins,
            "unique_users_today": unique_users_today,
        }
