## Test credentials (seeded by `python -m scripts.init --all`)

These are the dev/test credentials populated by the project's own init script
against the configured MongoDB. The default user was created via
`scripts/init_users.py` from the values in `/app/.env`.

- Email: `admin@example.com`
- Password: `admin12345`

### Auth flow used by the API

The API uses an **API-key → JWT** exchange (see `api/server/v1/auth.py`):

1. `GET /api/v1/auth/default-key` returns the seeded default API key.
2. `POST /api/v1/auth/token` with header `X-API-Key: <key>` returns an
   `access_token` (JWT) — pass it as `Authorization: Bearer <token>`.

The default user above is also valid for `POST /api/v1/auth/login`
(email + password) which returns the JWT directly.
