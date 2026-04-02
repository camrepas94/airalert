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
