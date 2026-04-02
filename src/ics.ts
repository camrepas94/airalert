/** Fold lines per RFC 5545 (max 75 octets; simplified for ASCII). */
function foldLine(line: string): string {
  const max = 75;
  if (line.length <= max) return line;
  let out = "";
  let rest = line;
  while (rest.length > max) {
    out += rest.slice(0, max) + "\r\n ";
    rest = rest.slice(max);
  }
  return out + rest;
}

function escapeText(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/;/g, "\\;").replace(/,/g, "\\,").replace(/\n/g, "\\n");
}

export type IcsEventInput = {
  uid: string;
  summary: string;
  description?: string;
  /** YYYY-MM-DD from TVMaze (calendar air date). */
  airdate: string;
};

export function buildIcsCalendar(title: string, events: IcsEventInput[]): string {
  const lines: string[] = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//Airalert//EN",
    "CALSCALE:GREGORIAN",
    "X-WR-CALNAME:" + escapeText(title),
  ];

  for (const ev of events) {
    const dt = ev.airdate.replace(/-/g, "");
    const stamp = formatUtcDateTime(new Date());
    lines.push(
      "BEGIN:VEVENT",
      "UID:" + escapeText(ev.uid),
      "DTSTAMP:" + stamp,
      "DTSTART;VALUE=DATE:" + dt,
      "DTEND;VALUE=DATE:" + addOneDayDate(dt),
      "SUMMARY:" + escapeText(ev.summary),
      ...(ev.description ? ["DESCRIPTION:" + escapeText(ev.description)] : []),
      "END:VEVENT",
    );
  }

  lines.push("END:VCALENDAR");
  return lines.map((l) => foldLine(l)).join("\r\n") + "\r\n";
}

function formatUtcDateTime(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    d.getUTCFullYear() +
    pad(d.getUTCMonth() + 1) +
    pad(d.getUTCDate()) +
    "T" +
    pad(d.getUTCHours()) +
    pad(d.getUTCMinutes()) +
    pad(d.getUTCSeconds()) +
    "Z"
  );
}

/** dt is YYYYMMDD; DTEND is exclusive next calendar day. */
function addOneDayDate(dt: string): string {
  const y = Number(dt.slice(0, 4));
  const m = Number(dt.slice(4, 6)) - 1;
  const d = Number(dt.slice(6, 8));
  const next = new Date(Date.UTC(y, m, d + 1));
  const pad = (n: number) => String(n).padStart(2, "0");
  return next.getUTCFullYear() + pad(next.getUTCMonth() + 1) + pad(next.getUTCDate());
}

export function episodeUid(showId: number, episodeId: number): string {
  return `airalert-${showId}-${episodeId}@airalert.local`;
}
