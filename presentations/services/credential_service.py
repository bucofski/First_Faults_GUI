from presentations.services.creadential import Credential, Role


class CredentialService:
    @staticmethod
    def get_current_credential() -> Credential:
        return Credential('superman', Role.ADMIN)
