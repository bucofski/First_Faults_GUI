from flask import session

from presentations.services.creadential import Credential, Role


class CredentialService:
    @staticmethod
    def get_current_credential() -> Credential | None:
        username = session.get("username")
        if not username:
            return None
        role_str = session.get("role", Role.USER.value)
        try:
            role = Role(role_str)
        except ValueError:
            role = Role.USER
        return Credential(username=username, role=role)
