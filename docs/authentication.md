# Authentication

Google Chat has no OAuth login for third-party clients, so the bridge logs in
with cookies copied from a browser logged into `chat.google.com`. Five are
required:
`COMPASS`, `SSID`, `SID`, `OSID`, `HSID`.

The rest of Google's session cookies are optional but recommended, and some
accounts (often consumer `@gmail.com` ones) are sent to the sign-in page
without them: `__Secure-1PSIDTS`, `__Secure-3PSIDTS`, `__Secure-1PSIDCC`,
`__Secure-3PSIDCC`, `SIDCC`, `APISID`, `SAPISID`, `__Secure-1PSID`,
`__Secure-3PSID`, `__Secure-1PAPISID`, `__Secure-3PAPISID`. The cURL paste
in step 2 picks all of them up for you.

> [!WARNING]
> These cookies are full session credentials: anyone holding them can use
> your Google account without your password or second factor. Paste them only
> into a private DM with the bridge bot, never into a public room, issue, config
> file or repository. If they leak, sign out of that browser session.

## 1. Copy the cookies

Use a private/incognito window and close it afterwards. A browser session
you keep using rotates the cookies and logs the bridge out; this is the most
common reason logins die early. Closing the window does not log the bridge
out.

1. Open a private window and log into https://chat.google.com.
2. Open DevTools (F12):
   - Chrome: **Application** → **Storage** → **Cookies** → `https://chat.google.com`
   - Firefox: **Storage** → **Cookies** → `https://chat.google.com`
3. Copy the **Value** of each of the five required cookies (and any of the
   optional ones you want to add). Some may be listed under
   `.google.com`; that's fine.
4. Close the private window.

## 2. Log in

Send `login` in a DM with the bridge bot (elsewhere use the command prefix,
e.g. `!gc login`), then reply with either:

- a JSON object (keys are case-sensitive; optional cookies are extra keys
  with their exact names, such as `"__Secure-1PSIDTS"`):

  ```json
  {"COMPASS": "<value>", "SSID": "<value>", "SID": "<value>", "OSID": "<value>", "HSID": "<value>"}
  ```

- or, better, a request copied with DevTools → **Network** → any
  `chat.google.com` request → **Copy as cURL (bash)**. The bridge takes the
  required and optional cookies and your browser's User-Agent from it. On Windows pick the bash
  variant; the cmd one isn't understood.

The bridge redacts your message, checks the cookies with Google and confirms
which account you logged in as. If a cookie is missing it says which one.

## When the login expires

Cookie logins can be invalidated by Google earlier than a normal browser
session. If the bridge reports the login expired, repeat the steps above with
a fresh private window.

Google rotates some cookies (the `*PSIDTS` and `*SIDCC` ones) while the
session is in use. The bridge stores a rotated cookie within about 30
seconds and again when it shuts down, so a restart continues with the
current values.
