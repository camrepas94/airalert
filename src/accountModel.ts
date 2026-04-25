/**
 * Account identity model — single source of truth lives in the `users` table:
 * - `auth_provider` — guest | local | google (bootstrap backfill in db.ts)
 * - `email`, `email_verified` — sign-in identity for local accounts
 * - `password_hash` — local credentials (not used for guest / pure Google)
 * - `google_sub` — Google subject when linked
 * - `username` — public handle (Community / DMs); required for most social features
 * - `is_admin` — staff tooling (separate from viewer "role" in userRole.ts)
 *
 * `AccountState` is always **derived in application code**, never a stored column, so we do not
 * introduce a second "kind" that can drift from auth fields.
 */
export type AuthProvider = "guest" | "local" | "google";

/** API-facing derived state for /api/users/me and admin (superset of auth flags). */
export type AccountState = "guest" | "google" | "email_local_google" | "legacy_local" | "email_local";

export function rowAuthProvider(
  row: { auth_provider?: string | null; authProvider?: string | null } | null | undefined,
): AuthProvider {
  const raw = row?.auth_provider ?? row?.authProvider;
  const v = String(raw ?? "local").toLowerCase();
  if (v === "guest") return "guest";
  if (v === "google") return "google";
  return "local";
}

function truthyHasPasswordFromRow(row: Record<string, unknown>): boolean {
  const h = row.hasPassword ?? row.hasPasswordForState ?? row.password_hash;
  if (typeof h === "boolean") return h;
  if (typeof h === "number") return h !== 0;
  return Boolean(h && String(h).trim());
}

function googleSubNonEmpty(row: Record<string, unknown>): boolean {
  const g = row.google_sub ?? row.googleSubInternal ?? row.googleSubForState;
  return typeof g === "string" && g.trim() !== "";
}

/**
 * Legacy username-only: local auth, non-empty password, non-empty username, no email.
 * Distinct from guest (`auth_provider === 'guest'`) and Google (`google` or non-empty `google_sub`).
 */
export function accountStateFromDbFields(row: Record<string, unknown>): AccountState {
  if (rowAuthProvider(row) === "guest") return "guest";
  if (rowAuthProvider(row) === "google") return "google";
  const email = row.email != null ? String(row.email).trim() : "";
  const username = row.username != null ? String(row.username).trim() : "";
  if (googleSubNonEmpty(row) && truthyHasPasswordFromRow(row)) return "email_local_google";
  if (googleSubNonEmpty(row)) return "google";
  if (rowAuthProvider(row) === "local" && truthyHasPasswordFromRow(row) && username !== "" && email === "")
    return "legacy_local";
  return "email_local";
}
