import { test } from "node:test";
import assert from "node:assert/strict";
import {
  dedupeBreakingNewsCandidates,
  headlineTokenJaccard,
  normalizeBreakingNewsUrl,
  type BreakingNewsDedupeRow,
} from "../src/breakingNewsDedupe.js";

function row(p: Partial<BreakingNewsDedupeRow> & Pick<BreakingNewsDedupeRow, "id" | "headline" | "source" | "url">): BreakingNewsDedupeRow {
  return {
    show_id: null,
    show_name: null,
    score: 80,
    snippet: null,
    created_at: "2026-04-10T12:00:00Z",
    ...p,
  };
}

test("normalized URL collapses duplicate tracking params", () => {
  const a = row({
    id: "a",
    headline: "Star fired from hit drama",
    source: "Deadline",
    url: "https://deadline.com/2026/04/story?utm_source=twitter",
    created_at: "2026-04-10T14:00:00Z",
  });
  const b = row({
    id: "b",
    headline: "Different words entirely about sports",
    source: "Variety",
    url: "https://deadline.com/2026/04/story",
    created_at: "2026-04-10T13:00:00Z",
  });
  const { kept, dropped } = dedupeBreakingNewsCandidates([a, b]);
  assert.equal(kept.length, 1);
  assert.equal(dropped.length, 1);
  assert.equal(kept[0].id, "a");
  assert.equal(dropped[0].reason, "normalized_url");
});

test("two outlets, nearly identical headlines same show — one kept", () => {
  const a = row({
    id: "a",
    headline: "Stranger Things star Millie Bobby Brown addresses wild season 5 fan theories",
    source: "TVLine",
    url: "https://tvline.com/a",
    show_id: 2993,
    show_name: "Stranger Things",
    created_at: "2026-04-10T15:00:00Z",
    score: 90,
  });
  const b = row({
    id: "b",
    headline: "Stranger Things star Millie Bobby Brown addresses wild season five fan theories",
    source: "Deadline",
    url: "https://deadline.com/b",
    show_id: 2993,
    show_name: "Stranger Things",
    created_at: "2026-04-10T15:05:00Z",
    score: 92,
  });
  const { kept } = dedupeBreakingNewsCandidates([a, b]);
  assert.equal(kept.length, 1);
  assert.equal(kept[0].source, "Deadline");
});

test("same show, clearly different topics — both kept", () => {
  const a = row({
    id: "a",
    headline: "Breaking Bad movie sequel announced for next year",
    source: "Variety",
    url: "https://variety.com/a",
    show_id: 169,
    show_name: "Breaking Bad",
    created_at: "2026-04-10T10:00:00Z",
  });
  const b = row({
    id: "b",
    headline: "Breaking Bad cast reunion panel scheduled at Comic-Con",
    source: "Deadline",
    url: "https://deadline.com/b",
    show_id: 169,
    show_name: "Breaking Bad",
    created_at: "2026-04-10T11:00:00Z",
  });
  const j = headlineTokenJaccard(a.headline, b.headline);
  assert.ok(j < 0.68, `expected low Jaccard, got ${j}`);
  const { kept } = dedupeBreakingNewsCandidates([a, b]);
  assert.equal(kept.length, 2);
});

test("slight rewording, same show, close time — duplicate", () => {
  const a = row({
    id: "a",
    headline: "The Last of Us season 3 adds major guest star from Succession",
    source: "TVLine",
    url: "https://tvline.com/x",
    show_id: 46562,
    show_name: "The Last of Us",
    created_at: "2026-04-10T16:00:00Z",
  });
  const b = row({
    id: "b",
    headline: "The Last of Us season 3 adds major guest star from succession cast",
    source: "EW",
    url: "https://ew.com/y",
    show_id: 46562,
    show_name: "The Last of Us",
    created_at: "2026-04-10T16:20:00Z",
  });
  const j = headlineTokenJaccard(a.headline, b.headline);
  assert.ok(j >= 0.68, `expected overlap, j=${j}`);
  const { kept } = dedupeBreakingNewsCandidates([a, b]);
  assert.equal(kept.length, 1);
});

test("canonical prefers higher tier when titles duplicate", () => {
  const worse = row({
    id: "w",
    headline: "Severance star talks finale shock and season 3 hopes",
    source: "TMZ",
    url: "https://tmz.com/w",
    show_id: 37136,
    show_name: "Severance",
    created_at: "2026-04-10T18:00:00Z",
    score: 95,
  });
  const better = row({
    id: "b",
    headline: "Severance star talks finale shock and season 3 hopes",
    source: "Variety",
    url: "https://variety.com/b",
    show_id: 37136,
    show_name: "Severance",
    created_at: "2026-04-10T17:00:00Z",
    score: 90,
  });
  const { kept } = dedupeBreakingNewsCandidates([worse, better]);
  assert.equal(kept.length, 1);
  assert.equal(kept[0].source, "Variety");
});

test("newer duplicate loses to better source when both match", () => {
  const olderBetterSource = row({
    id: "o",
    headline: "Yellowjackets EP teases season 4 timeline and new character",
    source: "Deadline",
    url: "https://deadline.com/o",
    show_id: 33671,
    show_name: "Yellowjackets",
    created_at: "2026-04-10T08:00:00Z",
  });
  const newerWeaker = row({
    id: "n",
    headline: "Yellowjackets EP teases season 4 timeline and new character",
    source: "Google News",
    url: "https://news.google.com/n",
    show_id: 33671,
    show_name: "Yellowjackets",
    created_at: "2026-04-10T20:00:00Z",
  });
  const { kept } = dedupeBreakingNewsCandidates([newerWeaker, olderBetterSource]);
  assert.equal(kept.length, 1);
  assert.equal(kept[0].source, "Deadline");
});

test("normalizeBreakingNewsUrl strips utm", () => {
  const u = "https://example.com/path/?utm_medium=social&id=1";
  assert.equal(normalizeBreakingNewsUrl(u).includes("utm_medium"), false);
});
