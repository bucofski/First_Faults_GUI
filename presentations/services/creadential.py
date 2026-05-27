from dataclasses import dataclass
from enum import Enum


class Role(Enum):
    """
    Application roles that control what a user is allowed to see and do.

    Attributes
    ----------
    ADMIN:
        Full access — can view all pages and trigger administrative actions.
    USER:
        Standard authenticated user with read access to all dashboards.
    GUEST:
        Limited read-only access; certain sensitive pages may be hidden.
    ANONYMOUS:
        Unauthenticated visitor.  Redirected to login for protected routes.
    """

    ADMIN = "admin"
    USER = "user"
    GUEST = "guest"
    ANONYMOUS = "anonymous"


@dataclass
class Credential:
    """
    Lightweight value object that holds the identity of the current user.

    Populated by :class:`~presentations.services.credential_service.CredentialService`
    and stored in the Flask session by the ``before_request`` hook in
    :func:`~presentations.app.create_app`.

    Attributes
    ----------
    username:
        The user's login name or display name.
    role:
        The user's :class:`Role`, used for access control decisions.
    """

    username: str
    role: Role
