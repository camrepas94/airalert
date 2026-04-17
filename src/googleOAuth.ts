/**
 * Google OAuth 2.0 (authorization code) helpers — token + userinfo via fetch (no extra npm deps).
 */

const GOOGLE_AUTH = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN = "https://oauth2.googleapis.com/token";
const GOOGLE_USERINFO = "https://www.googleapis.com/oauth2/v3/userinfo";

export function googleOAuthEnvReady(): boolean {
  return Boolean(
    process.env.AIRALERT_GOOGLE_CLIENT_ID?.trim() && process.env.AIRALERT_GOOGLE_CLIENT_SECRET?.trim(),
  );
}

export function googleAuthorizeUrl(opts: {
  clientId: string;
  redirectUri: string;
  state: string;
  scope?: string;
}): string {
  const params = new URLSearchParams({
    client_id: opts.clientId,
    redirect_uri: opts.redirectUri,
    response_type: "code",
    scope: opts.scope ?? "openid email profile",
    state: opts.state,
    access_type: "online",
    prompt: "select_account",
  });
  return `${GOOGLE_AUTH}?${params.toString()}`;
}

export async function exchangeGoogleAuthorizationCode(
  code: string,
  clientId: string,
  clientSecret: string,
  redirectUri: string,
): Promise<{ access_token: string }> {
  const body = new URLSearchParams({
    code,
    client_id: clientId,
    client_secret: clientSecret,
    redirect_uri: redirectUri,
    grant_type: "authorization_code",
  });
  const res = await fetch(GOOGLE_TOKEN, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Google token exchange failed (${res.status}): ${t.slice(0, 240)}`);
  }
  const json = (await res.json()) as { access_token?: string };
  if (!json.access_token || typeof json.access_token !== "string") {
    throw new Error("Google token response missing access_token");
  }
  return { access_token: json.access_token };
}

export async function fetchGoogleUserInfo(accessToken: string): Promise<{
  sub: string;
  email: string;
  emailVerified: boolean;
}> {
  const res = await fetch(GOOGLE_USERINFO, {
    headers: { Authorization: `Bearer ${accessToken}` },
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Google userinfo failed (${res.status}): ${t.slice(0, 240)}`);
  }
  const json = (await res.json()) as { sub?: string; email?: string; email_verified?: boolean | string };
  const sub = typeof json.sub === "string" ? json.sub.trim() : "";
  const email = typeof json.email === "string" ? json.email.trim().toLowerCase() : "";
  const emailVerified = json.email_verified === true || json.email_verified === "true";
  if (!sub || !email) {
    throw new Error("Google userinfo missing sub or email");
  }
  return { sub, email, emailVerified };
}
