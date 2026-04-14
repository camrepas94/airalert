export type OnboardingPrefs = {
  favoriteGenres: string[];
  favoriteNetworks: string[];
  setupCompletedAt: string | null;
};

const EMPTY: OnboardingPrefs = { favoriteGenres: [], favoriteNetworks: [], setupCompletedAt: null };

export function parseOnboardingPrefsJson(raw: string | null | undefined): OnboardingPrefs {
  if (raw == null || !String(raw).trim()) return { ...EMPTY };
  try {
    const o = JSON.parse(String(raw)) as Record<string, unknown>;
    const fg = Array.isArray(o.favoriteGenres)
      ? o.favoriteGenres.filter((x): x is string => typeof x === "string")
      : [];
    const fn = Array.isArray(o.favoriteNetworks)
      ? o.favoriteNetworks.filter((x): x is string => typeof x === "string")
      : [];
    const setup = typeof o.setupCompletedAt === "string" && o.setupCompletedAt.trim() ? o.setupCompletedAt.trim() : null;
    return {
      favoriteGenres: fg.map((g) => g.trim().toLowerCase()).filter((g) => g.length >= 2),
      favoriteNetworks: fn.map((n) => n.trim()).filter((n) => n.length >= 2),
      setupCompletedAt: setup,
    };
  } catch {
    return { ...EMPTY };
  }
}

export function serializeOnboardingPrefs(p: OnboardingPrefs): string {
  return JSON.stringify({
    favoriteGenres: p.favoriteGenres,
    favoriteNetworks: p.favoriteNetworks,
    setupCompletedAt: p.setupCompletedAt,
  });
}

/** Normalize PATCH input: cap lengths, lowercase genres. */
export function normalizeOnboardingPrefsInput(raw: Record<string, unknown>): OnboardingPrefs {
  const fgRaw = Array.isArray(raw.favoriteGenres) ? raw.favoriteGenres : [];
  const fnRaw = Array.isArray(raw.favoriteNetworks) ? raw.favoriteNetworks : [];
  const fg = fgRaw
    .filter((x): x is string => typeof x === "string")
    .map((g) => g.trim().toLowerCase())
    .filter((g) => g.length >= 2 && g.length <= 48)
    .slice(0, 12);
  const fn = fnRaw
    .filter((x): x is string => typeof x === "string")
    .map((n) => n.trim())
    .filter((n) => n.length >= 2 && n.length <= 64)
    .slice(0, 12);
  let setup: string | null = null;
  if (raw.setupCompletedAt === true) {
    setup = new Date().toISOString();
  } else if (typeof raw.setupCompletedAt === "string" && raw.setupCompletedAt.trim()) {
    setup = raw.setupCompletedAt.trim().slice(0, 40);
  }
  return { favoriteGenres: fg, favoriteNetworks: fn, setupCompletedAt: setup };
}
