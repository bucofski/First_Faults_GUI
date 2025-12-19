import logging
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Iterable, Optional

@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = False
    use_ssl: bool = False
    sender_email: Optional[str] = None  # fallback From address
    sender: Optional[str] = None

class MailService:
    """
    Handles email sending operations using SMTP configurations and provides tools for managing email content.

    This class serves as a utility for constructing and sending HTML-based emails with support for optional CC and BCC
    recipients. It supports SSL/TLS encryption and login functionality to secure email delivery.

    Attributes:
        config (SmtpConfig): The SMTP configuration that contains the necessary information for connecting to an email
            server, including host, port, sender email, username, and password.
    """
    def __init__(self, config: SmtpConfig = None):
        self.config = config
        self._logger = logging.getLogger(__name__)


    def send_html(
        self,
        to: Iterable[str] | str,
        subject: str,
        html_body: str,
        from_email: Optional[str] = None,
        cc: Optional[Iterable[str] | str] = None,
        bcc: Optional[Iterable[str] | str] = None,
    ) -> None:
        self._send_message(
            to=self._ensure_list(to),
            subject=subject,
            from_email=from_email
            or self.config.sender_email
            or (self.config.username or ""),
            html_body=html_body,
            cc=self._ensure_list(cc),
            bcc=self._ensure_list(bcc),
        )


    def _send_message(
        self,
        *,
        to: list[str],
        subject: str,
        from_email: str,
        html_body: str,
        cc: Optional[list[str]],
        bcc: Optional[list[str]],
    ) -> None:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = ", ".join(to)
        if cc:
            msg["Cc"] = ", ".join(cc)
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(self._fallback_plain(html_body), "plain", "utf-8"))
        alt.attach(MIMEText(html_body, "html", "utf-8"))
        msg.attach(alt)
        recipients = list({*to, *(cc or []), *(bcc or [])})
        self._deliver(from_email, recipients, msg)

    def _deliver(
        self, from_email: str, recipients: list[str], msg: MIMEMultipart
    ) -> None:
        """
        Sends an email message to the specified recipients using the configured server
        settings. Provides support for both SSL and TLS encryption based on the
        configuration and logs errors if the email fails to send.

        Parameters:
            from_email (str): The email address of the sender.
            recipients (list[str]): A list of recipient email addresses.
            msg (MIMEMultipart): The email message to be sent.

        Returns:
            None
        """
        try:
            if self.config.use_ssl:
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(
                    self.config.host, self.config.port, context=context
                ) as server:
                    self._maybe_login(server)
                    server.sendmail(from_email, recipients, msg.as_string())
            else:
                with smtplib.SMTP(self.config.host, self.config.port) as server:
                    if self.config.use_tls:
                        server.starttls(context=ssl.create_default_context())
                    self._maybe_login(server)
                    server.sendmail(from_email, recipients, msg.as_string())
        except Exception as e:
            logging.error(f"Failed to send email: {e}")

    def _maybe_login(self, server: smtplib.SMTP) -> None:
        """
        Logs in to the SMTP server if the necessary credentials are provided.

        This method attempts to log in to the SMTP server using the username
        and password specified in the configuration. If either the username
        or the password is absent, no login attempt is made.

        Args:
            server (smtplib.SMTP): The SMTP server instance to authenticate with.

        Returns:
            None
        """
        if self.config.username and self.config.password:
            server.login(self.config.username, self.config.password)

    @staticmethod
    def _ensure_list(val: Optional[Iterable[str] | str]) -> Optional[list[str]]:
        """
        Ensures that the given value is converted to a list of strings. If the input is already a list of strings, it is
        returned as is. If the input is a single string, it is wrapped in a list. If the input is None, None is returned.

        Args:
            val: The input value that can be a string, an iterable of strings, or None.

        Returns:
            A list of strings if the input was an iterable of strings or a single string wrapped in a list. Returns None
            if the input was None.
        """
        if val is None:
            return None
        if isinstance(val, str):
            return [val]
        return list(val)

    @staticmethod
    def _strip_html(html: str) -> str:
        # lightweight fallback; for better results use an HTML-to-text lib
        import re

        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)
        return text.strip()

    @staticmethod
    def _fallback_plain(html: str) -> str:
        return MailService._strip_html(html)






if __name__ == "__main__":
    cfg = SmtpConfig(
        host="192.168.0.174",
        port=25,
        username="benoit",
        password="R@nger&1401!",
        sender_email="benoit@albatros.be",
    )
    ms = MailService(cfg)
    start_dt = datetime.now() + timedelta(days=1)
    end_dt = start_dt + timedelta(hours=1)
    html = """
    <h2>Training Session</h2>
    <p>Hello,<br/>Please find below the meeting details.</p>
    <ul>
      <li>Topic: Fitness Assessment</li>
      <li>When: Tomorrow</li>
    </ul>
    """
    ms.send_html("person@example.com", "Plain HTML Test", html)
