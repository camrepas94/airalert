/**
 * Policy tests: expected HTTP status when calling admin APIs mirrors server replyForbiddenUnlessAdmin.
 * Run: npm test
 */
import { test } from "node:test";
import assert from "node:assert";

function expectedAdminApiStatus({ isRequestAdmin, sessionUserId }) {
  if (isRequestAdmin) return 200;
  if (!sessionUserId) return 401;
  return 403;
}

test("guest → 401 for admin API denial", () => {
  assert.strictEqual(expectedAdminApiStatus({ isRequestAdmin: false, sessionUserId: undefined }), 401);
});

test("signed-in non-admin → 403", () => {
  assert.strictEqual(expectedAdminApiStatus({ isRequestAdmin: false, sessionUserId: "u1" }), 403);
});

test("admin (session or env cookie) → 200", () => {
  assert.strictEqual(expectedAdminApiStatus({ isRequestAdmin: true, sessionUserId: "u1" }), 200);
  assert.strictEqual(expectedAdminApiStatus({ isRequestAdmin: true, sessionUserId: undefined }), 200);
});
