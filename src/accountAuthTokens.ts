import crypto, { randomUUID } from "node:crypto";
import { db } from "./db.js";

const EMAIL_VERIFY_EXPIRES_SQL = `datetime('now', '+72 hours')`;
const PASSWORD_RESET_EXPIRES_SQL = `datetime('now', '+1 hour')`;

export function hashOpaqueToken(raw: string): string {
  return crypto.createHash("sha256").update(raw, "utf8").digest("hex");
}

export function newOpaqueToken(): string {
  return crypto.randomBytes(32).toString("base64url");
}

export function storeEmailVerificationToken(userId: string, tokenHash: string): void {
  db.prepare(`DELETE FROM email_verification_tokens WHERE user_id = ?`).run(userId);
  db.prepare(
    `INSERT INTO email_verification_tokens (id, user_id, token_hash, expires_at) VALUES (?, ?, ?, ${EMAIL_VERIFY_EXPIRES_SQL})`,
  ).run(randomUUID(), userId, tokenHash);
}

/** Outcome of handling a raw token from the verification link (does not include empty token — handle in route). */
export type EmailVerificationTokenOutcome =
  | "verified"
  | "already_verified"
  | "expired"
  | "invalid";

/**
 * Marks email verified on first valid use; keeps the token row with `consumed_at` so repeat clicks can return
 * `already_verified`. Invalidates by expiry or wrong token. Tokens are replaced when a new verification email is sent.
 */
export function consumeEmailVerificationToken(rawToken: string): EmailVerificationTokenOutcome {
  const tokenHash = hashOpaqueToken(rawToken);
  const row = db
    .prepare(
      `SELECT user_id, consumed_at FROM email_verification_tokens WHERE token_hash = ?`,
    )
    .get(tokenHash) as { user_id: string; consumed_at: string | null } | undefined;
  if (!row) return "invalid";
  if (row.consumed_at) {
    const u = db
      .prepare(`SELECT (email_verified != 0) AS ev FROM users WHERE id = ?`)
      .get(row.user_id) as { ev: number } | undefined;
    return u?.ev ? "already_verified" : "invalid";
  }
  const notExpired = db
    .prepare(
      `SELECT 1 FROM email_verification_tokens WHERE token_hash = ? AND datetime(expires_at) > datetime('now')`,
    )
    .get(tokenHash);
  if (!notExpired) return "expired";
  const uid = row.user_id;
  const txn = db.transaction(() => {
    db.prepare(`UPDATE users SET email_verified = 1 WHERE id = ?`).run(uid);
    db.prepare(`UPDATE email_verification_tokens SET consumed_at = datetime('now') WHERE token_hash = ?`).run(tokenHash);
  });
  txn();
  return "verified";
}

export function storePasswordResetToken(userId: string, tokenHash: string): void {
  db.prepare(`DELETE FROM password_reset_tokens WHERE user_id = ?`).run(userId);
  db.prepare(
    `INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at) VALUES (?, ?, ?, ${PASSWORD_RESET_EXPIRES_SQL})`,
  ).run(randomUUID(), userId, tokenHash);
}

export function passwordResetTokenValid(rawToken: string): boolean {
  const tokenHash = hashOpaqueToken(rawToken);
  const hit = db
    .prepare(
      `SELECT 1 FROM password_reset_tokens WHERE token_hash = ? AND datetime(expires_at) > datetime('now')`,
    )
    .get(tokenHash);
  return Boolean(hit);
}

export function consumePasswordResetToken(
  rawToken: string,
  setPassword: (userId: string) => void,
): boolean {
  const tokenHash = hashOpaqueToken(rawToken);
  const row = db
    .prepare(
      `SELECT user_id FROM password_reset_tokens
       WHERE token_hash = ? AND datetime(expires_at) > datetime('now')`,
    )
    .get(tokenHash) as { user_id: string } | undefined;
  if (!row) return false;
  const uid = row.user_id;
  const txn = db.transaction(() => {
    db.prepare(`DELETE FROM password_reset_tokens WHERE user_id = ?`).run(uid);
    setPassword(uid);
  });
  txn();
  return true;
}
