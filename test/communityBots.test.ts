import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseAllowedWeekdaysJson,
  serializeAllowedWeekdays,
  isBotPostingDayAllowed,
  hasWeeklyCapacity,
  templateGeneratePostBody,
  plainPostHtml,
} from "../src/communityBotLogic.js";

test("parseAllowedWeekdaysJson defaults to all days on bad input", () => {
  assert.deepEqual(parseAllowedWeekdaysJson(""), [0, 1, 2, 3, 4, 5, 6]);
  assert.deepEqual(parseAllowedWeekdaysJson("not-json"), [0, 1, 2, 3, 4, 5, 6]);
});

test("serializeAllowedWeekdays dedupes and sorts", () => {
  assert.equal(serializeAllowedWeekdays([3, 1, 1, 5]), "[1,3,5]");
});

test("isBotPostingDayAllowed respects weekday list", () => {
  const monday = new Date("2026-05-25T18:00:00Z");
  assert.equal(isBotPostingDayAllowed([1], monday, "UTC"), true);
  assert.equal(isBotPostingDayAllowed([0], monday, "UTC"), false);
});

test("hasWeeklyCapacity enforces max", () => {
  assert.equal(hasWeeklyCapacity(2, 3), true);
  assert.equal(hasWeeklyCapacity(3, 3), false);
});

test("templateGeneratePostBody includes show and episode", () => {
  const body = templateGeneratePostBody({
    personaKey: "fan",
    showName: "RHOBH",
    episodeLabel: "S14E8",
    episodeSummary: "<p>DRAMA at the reunion.</p>",
  });
  assert.match(body, /RHOBH/);
  assert.match(body, /S14E8/);
});

test("plainPostHtml escapes markup", () => {
  assert.equal(plainPostHtml("<script>"), "<p>&lt;script&gt;</p>");
});
