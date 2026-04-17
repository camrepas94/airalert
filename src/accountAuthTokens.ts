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

/** Marks email verified and clears all verification rows for the user. Returns false if token missing or expired. */
export function consumeEmailVerificationToken(rawToken: string): boolean {
  const tokenHash = hashOpaqueToken(rawToken);
  const row = db
    .prepare(
      `SELECT user_id FROM email_verification_tokens
       WHERE token_hash = ? AND datetime(expires_at) > datetime('now')`,
    )
    .get(tokenHash) as { user_id: string } | undefined;
  if (!row) return false;
  const txn = db.transaction(() => {
    db.prepare(`UPDATE users SET email_verified = 1 WHERE id = ?`).run(row.user_id);
    db.prepare(`DELETE FROM email_verification_tokens WHERE user_id = ?`).run(row.user_id);
  });
  txn();
  return true;
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
