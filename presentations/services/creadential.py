from dataclasses import dataclass
from enum import Enum


class Role(Enum):
    ADMIN = "admin"
    USER = "user"
    GUEST = "guest"
    ANONYMOUS = "anonymous"

@dataclass
class Credential:
    username: str
    role: Role