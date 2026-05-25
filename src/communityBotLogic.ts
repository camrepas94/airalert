import crypto from "node:crypto";
import { safeTodayInTimeZone, sundayWeekStartContainingDate } from "./time.js";

export const BOT_SCHEDULE_TZ = "America/Los_Angeles";
export const DEFAULT_WEEKDAYS_JSON = "[0,1,2,3,4,5,6]";
export const PERSONA_KEYS = ["fan", "hype", "snark", "thoughtful"] as const;
export type PersonaKey = (typeof PERSONA_KEYS)[number];

export function parseAllowedWeekdaysJson(raw: string | null | undefined): number[] {
  if (!raw || !String(raw).trim()) return [0, 1, 2, 3, 4, 5, 6];
  try {
    const parsed = JSON.parse(String(raw)) as unknown;
    if (!Array.isArray(parsed)) return [0, 1, 2, 3, 4, 5, 6];
    const out = parsed
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n) && n >= 0 && n <= 6);
    return out.length ? [...new Set(out)].sort((a, b) => a - b) : [0, 1, 2, 3, 4, 5, 6];
  } catch {
    return [0, 1, 2, 3, 4, 5, 6];
  }
}

export function serializeAllowedWeekdays(days: number[]): string {
  const normalized = [...new Set(days.filter((d) => Number.isInteger(d) && d >= 0 && d <= 6))].sort((a, b) => a - b);
  return JSON.stringify(normalized.length ? normalized : [0, 1, 2, 3, 4, 5, 6]);
}

export function weekdayIndexSundayZero(date: Date, timeZone: string): number {
  const wd = new Intl.DateTimeFormat("en-US", { timeZone, weekday: "short" }).format(date);
  const map: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  for (const [k, v] of Object.entries(map)) {
    if (wd.startsWith(k)) return v;
  }
  return 0;
}

export function isBotPostingDayAllowed(
  allowedWeekdays: number[],
  now: Date = new Date(),
  timeZone = BOT_SCHEDULE_TZ,
): boolean {
  return allowedWeekdays.includes(weekdayIndexSundayZero(now, timeZone));
}

export function getBotWeekStartYmd(timeZone = BOT_SCHEDULE_TZ): string {
  const today = safeTodayInTimeZone(timeZone);
  return sundayWeekStartContainingDate(today, timeZone);
}

export function hasWeeklyCapacity(used: number, max: number): boolean {
  return used < Math.max(0, max);
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function plainPostHtml(text: string): string {
  return `<p>${escapeHtml(text.trim())}</p>`;
}

export function stripHtmlSummary(raw: string | null | undefined): string {
  if (!raw) return "";
  return raw
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstSentence(text: string, maxLen = 140): string {
  const t = text.trim();
  if (!t) return "";
  const m = t.match(/^(.+?[.!?])(?:\s|$)/);
  const sentence = m ? m[1].trim() : t.slice(0, maxLen);
  return sentence.length > maxLen ? `${sentence.slice(0, maxLen - 1).trim()}…` : sentence;
}

function pickTemplate(personaKey: string): string[] {
  const templates: Record<string, string[]> = {
    fan: [
      "Still thinking about {episodeLabel} of {showName}. {hook}",
      "{episodeLabel} of {showName} is living rent-free in my head. {hook}",
      "Just finished {episodeLabel} of {showName} and I need to talk about it. {hook}",
    ],
    hype: [
      "OK {showName} {episodeLabel} was INSANE. {hook}",
      "Everyone needs to watch {episodeLabel} of {showName} immediately. {hook}",
      "{showName} did NOT come to play in {episodeLabel}. {hook}",
    ],
    snark: [
      "So {episodeLabel} of {showName} happened… {hook}",
      "Watching {episodeLabel} of {showName} like 👀 {hook}",
      "{showName} {episodeLabel} had me yelling at the screen. {hook}",
    ],
    thoughtful: [
      "{episodeLabel} of {showName} raised a lot of questions for me. {hook}",
      "There is more going on in {episodeLabel} of {showName} than it seems. {hook}",
      "Curious what everyone thought of {episodeLabel} on {showName}. {hook}",
    ],
  };
  return templates[personaKey] ?? templates.fan;
}

function applyTemplate(template: string, vars: Record<string, string>): string {
  return template.replace(/\{(\w+)\}/g, (_, key: string) => vars[key] ?? "");
}

export function templateGeneratePostBody(opts: {
  personaKey: string;
  showName: string;
  episodeLabel: string;
  episodeSummary?: string | null;
}): string {
  const hook = firstSentence(stripHtmlSummary(opts.episodeSummary)) || "That ending though.";
  const templates = pickTemplate(opts.personaKey);
  const template = templates[crypto.randomInt(0, templates.length)];
  const body = applyTemplate(template, {
    showName: opts.showName,
    episodeLabel: opts.episodeLabel,
    hook,
  });
  return body.replace(/\s+/g, " ").trim().slice(0, 500);
}

export function templateGenerateReplyBody(opts: {
  personaKey: string;
  humanSnippet: string;
}): string {
  const snippet = opts.humanSnippet.replace(/\s+/g, " ").trim().slice(0, 120);
  const replies: Record<string, string[]> = {
    fan: ["Same!!", "Hard agree on this.", "You read my mind.", "This is exactly how I felt."],
    hype: ["YES.", "So glad someone said it!", "The energy in this thread is perfect."],
    snark: ["Not wrong.", "The way this is so accurate…", "Called it.", "Fair point."],
    thoughtful: ["Interesting take.", "Hadn't thought of it that way.", "Good point — what did you think of the ending?"],
  };
  const pool = replies[opts.personaKey] ?? replies.fan;
  const lead = pool[crypto.randomInt(0, pool.length)];
  if (!snippet) return lead;
  return `${lead} Re: "${snippet}"`.slice(0, 400);
}
