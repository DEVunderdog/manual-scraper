from datetime import datetime
from typing import Optional
from api.utils.application_timezone import get_current_time


class KeyInfo:
    def __init__(self, key: bytes, expires_at: Optional[datetime] = None):
        self.key = key
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        current_time = get_current_time()

        if self.expires_at and current_time > self.expires_at:
            return True
        else:
            return False
