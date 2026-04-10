/** Calendar date YYYY-MM-DD in an IANA timezone (used for episode "today" / upcoming). */
export function todayInTimeZone(timeZone: string): string {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date());
}

/** TVMaze may return full ISO timestamps; normalize to YYYY-MM-DD for SQLite and comparisons. */
export function normalizeEpisodeAirdate(raw: string | null | undefined): string | null {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

/** Same as {@link todayInTimeZone} but never throws on bad/empty zones (falls back to UTC calendar day). */
/** Subtract calendar days from a YYYY-MM-DD string (UTC date math). */
export function calendarDateMinusDays(isoYmd: string, days: number): string {
  const parts = isoYmd.split("-").map((x) => Number(x));
  const y = parts[0];
  const m = parts[1];
  const d = parts[2];
  if (!y || !m || !d) return isoYmd;
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() - days);
  return dt.toISOString().slice(0, 10);
}

/** Add calendar days to a YYYY-MM-DD string (same UTC calendar math as {@link calendarDateMinusDays}). */
export function calendarDatePlusDays(isoYmd: string, days: number): string {
  return calendarDateMinusDays(isoYmd, -days);
}

/**
 * A UTC instant that falls on the given calendar date in `timeZone` (for weekday / grid labels).
 * Uses hour stepping until `en-CA` formatting matches `ymd`.
 */
export function utcInstantForLocalCalendarDate(ymd: string, timeZone: string): number {
  const parts = ymd.split("-").map(Number);
  const y = parts[0];
  const mo = parts[1];
  const d = parts[2];
  if (!y || !mo || !d) return Date.now();
  let t = Date.UTC(y, mo - 1, d, 12, 0, 0);
  const dayFmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  for (let i = 0; i < 96; i++) {
    const got = dayFmt.format(new Date(t));
    if (got === ymd) return t;
    const cmp = got.localeCompare(ymd);
    if (cmp < 0) t += 3600000;
    else t -= 3600000;
  }
  return Date.UTC(y, mo - 1, d, 12, 0, 0);
}

/** Sunday (start of week) for the week containing `anchorYmd`, using `timeZone` for calendar days. */
export function sundayWeekStartContainingDate(anchorYmd: string, timeZone: string): string {
  let ymd = anchorYmd;
  const fmt = new Intl.DateTimeFormat("en-US", { timeZone, weekday: "short" });
  for (let i = 0; i < 7; i++) {
    const t = utcInstantForLocalCalendarDate(ymd, timeZone);
    const wd = fmt.format(new Date(t));
    if (wd.startsWith("Sun")) return ymd;
    ymd = calendarDateMinusDays(ymd, 1);
  }
  return anchorYmd;
}

export function safeTodayInTimeZone(timeZone: string | undefined | null): string {
  const z = (timeZone ?? "").trim();
  if (!z) {
    return new Date().toISOString().slice(0, 10);
  }
  try {
    return todayInTimeZone(z);
  } catch {
    try {
      return todayInTimeZone("UTC");
    } catch {
      return new Date().toISOString().slice(0, 10);
    }
  }
}
