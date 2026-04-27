import type { FastifyBaseLogger } from "fastify";

/**
 * Outbound transactional email (verification, password reset).
 * Provider HTTP calls live in sendLive() — not in routes or UI.
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
  const raw = process.env.AIRALERT_MAIL_PROVIDER;
  if (raw != null && String(raw).trim() !== "") {
    return String(raw).toLowerCase().trim();
  }
  if (process.env.RESEND_API_KEY?.trim() && process.env.EMAIL_FROM?.trim()) return "resend";
  return "none";
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
    if (!process.env.EMAIL_FROM?.trim()) {
      return { configured: false, provider, reason: "email_from_missing" };
    }
    return { configured: true, provider };
  }
  if (provider === "zoho" || provider === "zeptomail") {
    return { configured: false, provider, reason: "zeptomail_adapter_not_implemented" };
  }
  return { configured: false, provider, reason: "unknown_mail_provider" };
}

async function sendViaResend(log: FastifyBaseLogger, p: TransactionalMailPayload): Promise<TransactionalMailSendResult> {
  const apiKey = process.env.RESEND_API_KEY!.trim();
  const from = process.env.EMAIL_FROM!.trim();
  const body: Record<string, unknown> = {
    from,
    to: [p.to],
    subject: p.subject,
    text: p.textBody,
  };
  if (p.htmlBody) body.html = p.htmlBody;
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) {
    log.warn({ statusCode: res.status, subject: p.subject }, "[airalert-mail] Resend API rejected send");
    return { ok: false, code: "send_failed", error: `Resend HTTP ${res.status}` };
  }
  let json: unknown;
  try {
    json = JSON.parse(text) as { id?: string };
  } catch {
    log.warn({ subject: p.subject }, "[airalert-mail] Resend success body was not JSON");
    return { ok: false, code: "send_failed", error: "Resend response not JSON" };
  }
  const id = typeof json === "object" && json && "id" in json ? String((json as { id?: unknown }).id ?? "").trim() : "";
  if (!id) {
    log.warn({ subject: p.subject }, "[airalert-mail] Resend response missing id");
    return { ok: false, code: "send_failed", error: "Resend response missing message id" };
  }
  log.info({ resendMessageId: id, subject: p.subject }, "[airalert-mail] Resend accepted send");
  return { ok: true };
}

async function sendLive(
  log: FastifyBaseLogger,
  p: TransactionalMailPayload,
): Promise<TransactionalMailSendResult> {
  const provider = configuredProvider();
  if (provider === "resend" && process.env.RESEND_API_KEY?.trim() && process.env.EMAIL_FROM?.trim()) {
    return sendViaResend(log, p);
  }
  if (provider === "zoho" || provider === "zeptomail") {
    return { ok: false, code: "adapter_missing", error: "Zoho/ZeptoMail adapter not implemented yet" };
  }
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
    if (!r.ok) log.warn({ err: r.error, code: r.code, subject: p.subject }, "[airalert-mail] send failed");
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
