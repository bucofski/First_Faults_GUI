"""Generate a werkzeug password hash for the auth user store.

Usage:
    python scripts/hash_password.py <username> <role>
    (password is read from stdin, not echoed)

Then put the printed JSON into AUTH_USERS_JSON or AUTH_USERS_FILE.
"""
import getpass
import json
import sys

from werkzeug.security import generate_password_hash


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    username, role = sys.argv[1], sys.argv[2]
    pw = getpass.getpass("Password: ")
    pw2 = getpass.getpass("Confirm:  ")
    if pw != pw2:
        print("Passwords do not match.", file=sys.stderr)
        return 1
    print(json.dumps({username: {"password_hash": generate_password_hash(pw), "role": role}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
