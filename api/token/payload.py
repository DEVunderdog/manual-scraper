from pydantic import BaseModel
from api.types.roles import Role


class PayloadData(BaseModel):
    user_id: int
    role: Role


class TokenData(PayloadData):
    pass


class ApiData(PayloadData):
    api_key_id: int
