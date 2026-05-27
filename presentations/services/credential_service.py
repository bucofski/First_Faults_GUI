from presentations.services.creadential import Credential, Role


class CredentialService:
    """
    Resolve the credential of the currently authenticated user.

    This service acts as the single point of truth for "who is logged in?"
    within the presentation layer.  The ``before_request`` hook in
    :func:`~presentations.app.create_app` calls
    :meth:`get_current_credential` on every request and writes the result
    into the Flask session.

    .. note::
        The current implementation is a **stub** that always returns a
        hardcoded ``superman / ADMIN`` credential.  It will be replaced by
        real session-based or OAuth credential resolution once the
        authentication flow is implemented.
    """

    @staticmethod
    def get_current_credential() -> Credential:
        """
        Return the credential for the active user.

        Returns
        -------
        Credential
            The :class:`~presentations.services.creadential.Credential`
            representing the logged-in user, or an anonymous credential
            when no user is authenticated.

        .. warning::
            Currently hardcoded — replace with real auth lookup before
            deploying to production.
        """
        return Credential('superman', Role.ADMIN)
