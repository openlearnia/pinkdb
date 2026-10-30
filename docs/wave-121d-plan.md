# Wave 121d — the console's authentication (plan)

Started and done 2026-10-30. The user asked for "proper authentication for the Web
UI" and chose: sign-in by default with a first-run setup, account
self-service, personal access tokens, HTTPS for the console, and TOTP
two-factor.

## What exists (wave 78, 121)

Logins verify through the engine's Argon2id path (`Connection::login`);
statements run as the user (`impersonate`). Sessions are server-side
(256-bit cookie, `HttpOnly; SameSite=Strict`, a separate CSRF token, 30 min
idle / 12 h absolute). Failed logins are throttled per address. The gaps:

- A store without users is served **without** sign-in, and sign-in is
  opt-in (`--ui-auth basic`); creating the first user in the console ends
  it until a restart with that flag.
- `--ui-token` is one shared secret that signs in as an anonymous operator.
- HTTP Basic on every API request (scripts) would bypass a second factor.
- No password change, no session list, no sign-out-everywhere.
- No HTTPS, so the console is loopback-only.

## Decisions

1. **Sign-in by default.** `UiAuth::Password` is the default (`--ui-auth`
   is now `password` or `none`). `none` is an explicit opt-in for a store
   without users on loopback, as today. `UiAuth::Token` and `--ui-token`
   are removed (no legacy compatibility).
2. **First-run setup.** Password mode on a store without users starts in
   setup: `GET /api/session` answers `401 {auth: "setup"}`, and
   `POST /api/setup {code, user, password}` creates the first user with
   `GRANT ADMIN` and signs them in. The **setup code** (12 base32
   characters, 60 bits) is generated at bind, printed to the server's
   stderr and returned by `UiServer::setup_code()`; it is compared in
   constant time, throttled like a login, and void once the store has a
   user. Passwords need at least 10 characters.
3. **Header credentials: personal access tokens only.** HTTP Basic on the
   API is removed. A token is `pdbt_` + 43 random characters, shown once;
   the store keeps its SHA-256, a 4-character prefix for recognition, a
   name, the creation, expiry (30, 90, 365 days or never) and last-use
   time. `Authorization: Bearer pdbt_…` works on every `/api` route and
   the browse API, as the token's owner (no CSRF: no cookie). Revoking or
   dropping the user ends it. At most 50 per user.
4. **TOTP (RFC 6238).** HMAC-SHA1 (ring), 6 digits, 30 s steps, one step of
   drift either way, and a used step is never accepted twice. Enrolment:
   `start` returns the base32 secret, the `otpauth://` URI and a QR code
   (qrcodegen, rendered as one SVG path); `confirm` with a code turns it on
   and returns 10 one-time recovery codes (stored hashed). Sign-in with
   2FA: the password step answers `{second_factor: "totp", challenge}` (5
   minutes, 5 tries, same address); `POST /api/login/verify` takes a code
   or a recovery code. Turning it off needs the password; an admin can
   reset it for a user.
5. **Sessions are visible.** Each has a public id, the address, the user
   agent and its start. A user sees and ends their own sessions (or all
   others); an admin ends all of a user's sessions. A password change,
   a password reset, a 2FA reset or dropping the user ends the user's other
   sessions.
6. **Self-service password change.** `ALTER USER` is admin-only in the
   engine, so the console checks the current password through the engine
   and then runs `ALTER USER` on a trusted connection (the worksheet-store
   pattern).
7. **HTTPS.** `UiOptions::tls` (PEM certificate chain and key;
   `--ui-tls-cert`, `--ui-tls-key`). The accept loop wraps connections in
   tokio-rustls (ring provider); the handshake counts against the request
   deadline. With TLS: `Secure` cookies, `Strict-Transport-Security`, and
   the bind policy allows a non-loopback address — but never with
   `--ui-auth none`.
8. **Where the state lives.** `pinkdb_worksheets.access_tokens`,
   `.totp` and `.recovery_codes`, written on trusted connections with bound
   parameters. Non-admins have no grant on that database; administrators
   can already reset any credential.

## The app

- Sign-in screen with three steps: setup (code, name, password, confirm),
  password, and the second factor (code, or "use a recovery code").
- An **Account** page (`/account`, from the account menu): password,
  two-factor (QR, confirm, recovery codes shown once), sessions, access
  tokens (the new token shown once with Copy).
- Access page: a 2FA badge and per-user "Sign out everywhere" and "Reset
  two-factor" for administrators.

## Tests

Server: setup (wrong code, short password, second setup refused), login
with and without TOTP (wrong code, replayed code, recovery code once),
challenge expiry and attempts, PAT use/expiry/revocation and no CSRF need,
Basic header refused, password change ending other sessions, session list
and revoke, admin reset, TLS round trip with an rcgen certificate, bind
policy with TLS, `none` refused with users. Unit: TOTP against the RFC 6238
test vectors, base32, token parsing. App: route and form helpers.
