import crypto from "node:crypto";

const SCRYPT_PARAMS = { N: 16384, r: 8, p: 1, maxmem: 64 * 1024 * 1024 } as const;

export function hashPassword(plain: string): string {
  const salt = crypto.randomBytes(16).toString("hex");
  const derived = crypto.scryptSync(plain, salt, 64, SCRYPT_PARAMS);
  return `scrypt16$${salt}$${derived.toString("hex")}`;
}

export function verifyPassword(plain: string, stored: string | null | undefined): boolean {
  if (!stored || typeof stored !== "string") return false;
  const parts = stored.split("$");
  if (parts.length !== 3 || parts[0] !== "scrypt16") return false;
  const [, salt, hashHex] = parts;
  if (!salt || !hashHex) return false;
  let expected: Buffer;
  try {
    expected = Buffer.from(hashHex, "hex");
  } catch {
    return false;
  }
  let derived: Buffer;
  try {
    derived = crypto.scryptSync(plain, salt, 64, SCRYPT_PARAMS);
  } catch {
    return false;
  }
  if (expected.length !== derived.length) return false;
  try {
    return crypto.timingSafeEqual(expected, derived);
  } catch {
    return false;
  }
}
