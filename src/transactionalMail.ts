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

export type TransactionalMailSendResult =
  | { ok: true }
  | { ok: false; code: "not_configured" | "adapter_missing" | "send_failed"; error: string };

function configuredProvider(): string {
  return (process.env.AIRALERT_MAIL_PROVIDER ?? "none").toLowerCase().trim();
}

export function transactionalMailStatus(): { configured: boolean; provider: string; reason?: string } {
  const provider = configuredProvider();
  if (!provider || provider === "none") {
    return { configured: false, provider: provider || "none", reason: "mail_provider_not_configured" };
  }
  if (provider === "log") {
    return { configured: false, provider, reason: "log_only_mail_is_not_delivery" };
  }
  if (provider === "resend") {
    if (!process.env.RESEND_API_KEY?.trim()) {
      return { configured: false, provider, reason: "resend_api_key_missing" };
    }
    return { configured: false, provider, reason: "resend_adapter_not_implemented" };
  }
  if (provider === "zoho" || provider === "zeptomail") {
    return { configured: false, provider, reason: "zeptomail_adapter_not_implemented" };
  }
  return { configured: false, provider, reason: "unknown_mail_provider" };
}

async function sendLive(
  log: FastifyBaseLogger,
  p: TransactionalMailPayload,
): Promise<TransactionalMailSendResult> {
  const provider = configuredProvider();
  if (provider === "resend" && process.env.RESEND_API_KEY?.trim()) {
    return { ok: false, code: "adapter_missing", error: "Resend HTTP adapter not implemented yet" };
  }
  if (provider === "zoho" || provider === "zeptomail") {
    return { ok: false, code: "adapter_missing", error: "Zoho/ZeptoMail adapter not implemented yet" };
  }
  void log;
  void p;
  return { ok: false, code: "not_configured", error: `Live send not configured for AIRALERT_MAIL_PROVIDER=${provider}` };
}

export function createTransactionalMailer(log: FastifyBaseLogger) {
  const provider = configuredProvider();

  async function send(p: TransactionalMailPayload): Promise<TransactionalMailSendResult> {
    const status = transactionalMailStatus();
    if (!status.configured) {
      log.warn(
        { provider, reason: status.reason, subject: p.subject },
        "[airalert-mail] transactional email not sent because delivery is not configured",
      );
      return {
        ok: false,
        code: status.reason?.includes("adapter") ? "adapter_missing" : "not_configured",
        error: status.reason ?? "mail_not_configured",
      };
    }
    if (provider === "log") {
      return { ok: false, code: "not_configured", error: "log_only_mail_is_not_delivery" };
    }
    const r = await sendLive(log, p);
    if (!r.ok) log.warn({ err: r.error, code: r.code, subject: p.subject }, "[airalert-mail] send failed or adapter missing");
    return r;
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
