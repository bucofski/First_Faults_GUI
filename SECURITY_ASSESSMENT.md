# Security Assessment — First Faults GUI

**Date:** 2026-05-07
**Branch:** `52-security-assement`
**Frameworks:** OWASP Top 10 (2021), NIST Cybersecurity Framework 2.0
**Assessor:** Automated code review

---

## Executive Summary

The First Faults GUI is a Flask-based three-tier application using SQLAlchemy ORM against MS SQL Server. The architecture is sound (parameterized queries, layered design), but the security implementation is incomplete and the project carries significant credential-exposure risk. The project is currently in MVP stage on the `52-security-assement` branch.

**Overall risk rating: HIGH** — driven primarily by hardcoded credentials in version control and the absence of real authentication/authorization.

| Severity | Count |
|---|---|
| Critical | 4 |
| High | 3 |
| Medium | 4 |
| Low / Informational | 3 |

---

## OWASP Top 10 (2021)

### A01 — Broken Access Control — **CRITICAL**

- No authorization checks on any Flask routes (`presentations/routes/plc_routes.py`); `home`, `diagrams`, `table_tree`, and PDF export endpoints are all publicly reachable.
- A `Role` enum is defined in `presentations/services/creadential.py:5-9` but never enforced anywhere in the request pipeline (no decorators, no middleware).
- `presentations/services/credential_service.py:7` always returns `Credential('superman', Role.ADMIN)` — every visitor is implicitly an admin.

**Remediation:** Implement a real auth backend (Flask-Login or similar), add `@login_required` / role-check decorators to every route, and verify session role server-side on each request.

### A02 — Cryptographic Failures / Sensitive Data Exposure — **CRITICAL**

- Database credentials in plaintext YAML at `config/Connection.yaml:5-6` (user `sa`, password `Test@1234`).
- SMTP credentials hardcoded at `business/utils/mail_service.py:168-175`.
- Flask `secret_key` set to the literal string `"dev"` at `presentations/app.py:20`.
- Connection string uses `Encrypt=true` but `trust_server_certificate=true` (`config/Connection.yaml:8-9`), which negates certificate validation.
- No HTTPS configuration documented; Flask serves over HTTP by default.

**Remediation:** Move secrets into environment variables or a vault (python-dotenv, Azure Key Vault, AWS Secrets Manager). Rotate all currently-committed credentials immediately. Disable `trust_server_certificate` and install a proper CA chain. Terminate TLS at IIS / a reverse proxy in production.

### A03 — Injection — **LOW**

- SQLAlchemy ORM is used consistently across `data/repositories/repository.py` and `data/repositories/snapshot_repository.py`; queries are parameterized.
- The table-valued function call `func.dbo.fn_InterlockChain()` at `presentations/routes/plc_routes.py:47-54` binds parameters via SQLAlchemy.
- No raw SQL string concatenation observed.

**Note:** String filters (`filter_condition_message`, `filter_plc`) are passed through without length or character-class validation. Risk is mitigated by ORM binding, but adding allowlist validation is recommended.

### A04 — Insecure Design — **HIGH**

- Authentication is stubbed and authorization is undesigned — these are architectural gaps, not bugs.
- No threat model is documented in `docs/`.
- No rate limiting, CAPTCHA, or anti-automation controls on any endpoint.

### A05 — Security Misconfiguration — **CRITICAL**

- Jinja2 `autoescape` is enabled globally (`presentations/app.py:21`) but bypassed via `|safe` on Plotly chart HTML in `presentations/templates/diagrams.html` and `presentations/templates/home.html`.
- No Content Security Policy, `X-Frame-Options`, `X-Content-Type-Options`, or HSTS headers are set.
- Error handlers log full stack traces, which can leak file paths and internals if surfaced to the client.
- Auth log at `config/logging_config.py:55-58` writes locally, not to a centralized log sink.

### A06 — Vulnerable & Outdated Components — **MEDIUM**

- `pyproject.toml` uses loose lower-bound pins: `flask[all]>=3.1.2`, `torch>=2.9.1`, `scikit-learn>=1.8.0`. No upper bounds.
- `uv.lock` is present, which helps reproducibility, but no automated dependency-vulnerability scanning is wired into CI.

**Remediation:** Add `pip-audit` or `safety` to CI; pin upper bounds where API stability matters; review the ML stack (`torch`, `scikit-learn`) — it adds large attack surface to a web app.

### A07 — Identification & Authentication Failures — **CRITICAL**

- See A01: there is no real login flow. The session user is hardcoded.
- No password policy, MFA, account lockout, or session timeout.

### A08 — Software & Data Integrity Failures — **LOW**

- No insecure deserialization observed (no `pickle.loads` on untrusted data).
- No code-signing / package-integrity verification beyond `uv.lock`.

### A09 — Security Logging & Monitoring Failures — **MEDIUM**

- Logging infrastructure exists (`config/logging_config.py`) but:
  - Only ERROR level is captured for most components.
  - No audit trail for data access or modifications.
  - No login-failure or authorization-denial logging.
  - `run_daily_snapshot.py` does not log success/failure of the snapshot job.
- No SIEM or centralized log aggregation.

### A10 — Server-Side Request Forgery (SSRF) — **NOT OBSERVED**

No outbound HTTP fetches based on user input were identified.

### Additional — Cross-Site Request Forgery — **HIGH**

- No CSRF protection (no Flask-WTF, no token validation on POST forms). State-changing endpoints are unprotected.

---

## NIST Cybersecurity Framework 2.0

CSF 2.0 introduces **Govern** as a sixth function alongside Identify, Protect, Detect, Respond, and Recover. Coverage estimates below are qualitative.

### GV — Govern — **~5%**

- No security policy, risk register, or ownership documented.
- No secrets-management policy; credentials live in the repo.
- `CODE_OF_CONDUCT.md` exists but is not a security governance artifact.

**Recommendation:** Author a short `SECURITY.md` defining responsible-disclosure contact, a `THREAT_MODEL.md`, and a secrets-handling policy.

### ID — Identify — **~30%**

- Architecture and data model are documented (`Database.md`, `docs/HighLevelArchitectureDiagram.png`, `PROJECT_PROGRESS.md`).
- No formal asset inventory, data-classification, or risk assessment.
- No threat model.

### PR — Protect — **~25%**

- ✅ ORM parameterization prevents SQL injection.
- ✅ Jinja2 autoescape is on by default.
- ✅ DB encryption-in-transit is requested (though cert validation is disabled).
- ❌ No application-level secrets management.
- ❌ No authentication, authorization, CSRF, or rate limiting.
- ❌ No security headers / CSP.

### DE — Detect — **~10%**

- File-based error logging only.
- No intrusion-detection, anomaly-detection, or alerting.
- No health checks on the daily snapshot job.

### RS — Respond — **~0%**

- No incident response plan, runbook, or escalation path.
- `MailService` exists and could feed alerts but is not wired to security events.

### RC — Recover — **~10%**

- 90-day retention is configured (`data/repositories/snapshot_repository.py`, `RETENTION_DAYS = 90`) for reporting, not disaster recovery.
- No documented backup strategy, restore procedure, or DR plan.
- IIS deployment is still pending (`PROJECT_PROGRESS.md:99`).

---

## Prioritized Remediation Plan

### Immediate (this week)

1. **Rotate every credential currently in the repo.** Treat `sa / Test@1234` and the SMTP password as compromised.
2. Remove `config/Connection.yaml` credentials and the SMTP block from source control; load from environment variables. Add a `.env.example` instead.
3. Replace `secret_key = "dev"` with a value loaded from the environment.
4. Add `config/Connection.yaml` and `.env` to `.gitignore` (verify with `git check-ignore`).
5. Audit `git log` for the credentials and consider repo history rewrite (`git filter-repo`) plus force-push coordination.

### Short term (this sprint)

6. Implement real authentication (Flask-Login + a hashed-password store) and remove the `superman` stub.
7. Add `@login_required` and a role-check decorator on every route in `presentations/routes/plc_routes.py`.
8. Add CSRF protection via Flask-WTF.
9. Set security headers (CSP, HSTS, X-Frame-Options, X-Content-Type-Options) — Flask-Talisman is the simplest path.
10. Add `pip-audit` to CI.

### Medium term (this quarter)

11. Centralize logging (file → syslog / ELK / Azure Monitor) and add login-failure / authorization-denial events.
12. Author `SECURITY.md`, `THREAT_MODEL.md`, and an incident-response runbook.
13. Document and test backup/restore for the SQL Server database.
14. Set `trust_server_certificate=false` and deploy a proper CA-issued certificate on the SQL host.
15. Add rate limiting (Flask-Limiter) on auth and export endpoints.

---

## Appendix — Files Cited

- `config/Connection.yaml`
- `config/logging_config.py`
- `presentations/app.py`
- `presentations/routes/plc_routes.py`
- `presentations/services/credential_service.py`
- `presentations/services/creadential.py`
- `presentations/templates/diagrams.html`, `home.html`, `table_tree.html`
- `business/utils/mail_service.py`
- `data/repositories/repository.py`
- `data/repositories/snapshot_repository.py`
- `pyproject.toml`, `uv.lock`
- `run_daily_snapshot.py`
