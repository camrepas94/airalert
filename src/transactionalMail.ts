import type { FastifyBaseLogger } from "fastify";

/**
 * Outbound transactional email (verification, password reset).
 * Wire Resend, Zoho ZeptoMail, etc. in sendLive() — keep all provider HTTP/SDK calls here (not in routes or UI).
 */
export type TransactionalMailPayload = {
  to: string;
  subject: string;
  textBody: string;
  htmlBody?: string;
};

async function sendLive(
  log: FastifyBaseLogger,
  p: TransactionalMailPayload,
): Promise<{ ok: true } | { ok: false; error: string }> {
  const provider = (process.env.AIRALERT_MAIL_PROVIDER ?? "log").toLowerCase().trim();
  if (provider === "resend" && process.env.RESEND_API_KEY?.trim()) {
    return { ok: false, error: "Resend HTTP adapter not implemented yet — use AIRALERT_MAIL_PROVIDER=log until wired in transactionalMail.ts" };
  }
  if (provider === "zoho" || provider === "zeptomail") {
    return { ok: false, error: "Zoho/ZeptoMail adapter not implemented yet" };
  }
  void log;
  void p;
  return { ok: false, error: `Live send not configured for AIRALERT_MAIL_PROVIDER=${provider}` };
}

export function createTransactionalMailer(log: FastifyBaseLogger) {
  const provider = (process.env.AIRALERT_MAIL_PROVIDER ?? "log").toLowerCase().trim();

  async function send(p: TransactionalMailPayload): Promise<void> {
    if (provider === "none") return;
    if (provider === "log") {
      log.info(
        { to: p.to, subject: p.subject, textPreview: p.textBody.slice(0, 500) },
        "[airalert-mail] log-only — set AIRALERT_MAIL_PROVIDER=resend + RESEND_API_KEY when ready (see transactionalMail.ts sendLive)",
      );
      return;
    }
    const r = await sendLive(log, p);
    if (!r.ok) log.warn({ err: r.error, to: p.to }, "[airalert-mail] send failed or adapter missing");
  }

  return {
    sendVerificationEmail(to: string, verifyUrl: string) {
      return send({
        to,
        subject: "Verify your AirAlert email",
        textBody: `Verify your email for AirAlert by opening this link (or paste into your browser):\n\n${verifyUrl}\n\nIf you did not sign up, you can ignore this message.`,
      });
    },
    sendPasswordResetEmail(to: string, resetUrl: string) {
      return send({
        to,
        subject: "Reset your AirAlert password",
        textBody: `Reset your AirAlert password using this link. It expires in about one hour:\n\n${resetUrl}\n\nIf you did not request a reset, ignore this email.`,
      });
    },
  };
}
