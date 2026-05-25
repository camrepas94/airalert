#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const preview = fs.readFileSync(path.join(root, "public/previews/episode-reactor-preview.html"), "utf8");

const styleMatch = preview.match(/<style>([\s\S]*?)<\/style>/);
if (!styleMatch) throw new Error("missing style");
let css = styleMatch[1];

for (const re of [
  /\/\* Preview chrome[\s\S]*?\.preview-state-btns button\.active \{[\s\S]*?\}\s*/m,
  /\.app-frame \{[\s\S]*?\}\s*/m,
  /\/\* AirAlert app chrome[\s\S]*?\.aa-chrome-note \{[\s\S]*?\}\s*/m,
  /\/\* Algorithm legend[\s\S]*?\.algo-legend summary \{[\s\S]*?\}\s*/m,
  /\/\* Composer modal[\s\S]*?\.composer-suggest strong \{[\s\S]*?\}\s*/m,
  /:root \{[\s\S]*?\}\s*/m,
  /\*, \*::before, \*::after \{[\s\S]*?\}\s*/m,
  /html, body \{[\s\S]*?\}\s*/m,
]) {
  css = css.replace(re, "");
}

css = css
  .replace(/var\(--cyan\)/g, "var(--sh-cyan, var(--neon-cyan, #22d3ee))")
  .replace(/var\(--purple\)/g, "var(--sh-purple, #a855f7)")
  .replace(/var\(--magenta\)/g, "var(--sh-magenta, var(--neon-magenta, #e879f9))")
  .replace(/var\(--card\)/g, "var(--panel-solid, #0a0a0f)")
  .replace(/var\(--gradient-border\)/g, "var(--sh-gradient-border, linear-gradient(135deg, rgba(34, 211, 238, 0.55), rgba(168, 85, 247, 0.45)))");

const scope = "#tab-community .social-hub ";
css = css.replace(/^(\s*)(\.[a-zA-Z#][^{]*\{)/gm, (m, indent, sel) => {
  const t = sel.trim();
  if (t.startsWith("@") || t.startsWith("#tab-community")) return m;
  return `${indent}${scope}${t}`;
});

const hubLayout = `#tab-community .social-hub {
  --sh-cyan: var(--neon-cyan, #22d3ee);
  --sh-purple: #a855f7;
  --sh-magenta: var(--neon-magenta, #e879f9);
  --sh-gradient-border: linear-gradient(135deg, rgba(34, 211, 238, 0.55), rgba(168, 85, 247, 0.45));
  --safe-top: env(safe-area-inset-top, 0px);
  --safe-bottom: env(safe-area-inset-bottom, 0px);
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  min-height: 0;
  padding: 0;
  gap: 0;
  overflow: hidden;
  background:
    radial-gradient(ellipse 80% 50% at 20% 0%, rgba(168, 85, 247, 0.08), transparent 55%),
    radial-gradient(ellipse 70% 45% at 90% 15%, rgba(34, 211, 238, 0.06), transparent 50%);
}
#tab-community .community-root {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  min-height: 0;
  padding-bottom: 0;
}
#tab-community .community-list-view.social-hub { padding-bottom: 0; }
#tab-community .social-hub .community-general-list.social-reactor-threads {
  list-style: none; margin: 0 0 1rem; padding: 0;
  display: flex; flex-direction: column; gap: 0.55rem;
}
#tab-community .social-hub .community-general-list.social-reactor-threads .community-thread-card {
  width: 100%; text-align: left;
}
#tab-community .social-hub .reactor-discussions-heading {
  margin: 0 0 0.45rem; font-size: 0.72rem; font-weight: 800;
  letter-spacing: 0.07em; text-transform: uppercase; color: var(--sh-cyan, var(--neon-cyan));
}
#tab-community .social-hub .reactor-discussions-err { font-size: 0.75rem; margin: 0 0 0.65rem; }
#tab-community .social-hub .gossip-skeleton-hero {
  min-height: 200px; border-radius: 16px; margin-bottom: 0.75rem;
  background: linear-gradient(90deg, rgba(255,255,255,0.04), rgba(255,255,255,0.1), rgba(255,255,255,0.04));
  background-size: 200% 100%; animation: socialHubShimmer 1.4s ease-in-out infinite;
}
#tab-community .social-hub .gossip-skeleton-item {
  height: 88px; border-radius: 14px; margin-bottom: 0.55rem;
  background: linear-gradient(90deg, rgba(255,255,255,0.04), rgba(255,255,255,0.1), rgba(255,255,255,0.04));
  background-size: 200% 100%; animation: socialHubShimmer 1.4s ease-in-out infinite;
}
@keyframes socialHubShimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}
#tab-community .social-hub .gossip-empty {
  text-align: center; padding: 2rem 1rem; color: var(--muted); font-size: 0.82rem;
}
body:has(#tab-community.tab-panel.active #communityListView[data-social-section="watch"]) .community-fab-layer,
body:has(#tab-community.tab-panel.active #communityListView[data-social-section="diary"]) .community-fab-layer,
body:has(#tab-community.tab-panel.active #communityListView[data-social-section="news"]) .community-fab-layer {
  display: none !important;
}
`;

fs.writeFileSync(path.join(root, "public/social-hub.css"), `/* Social hub — Community tab */\n${hubLayout}\n${css}\n`);

function sliceBetween(startMarker, endMarker) {
  const a = preview.indexOf(startMarker);
  const b = preview.indexOf(endMarker);
  if (a < 0 || b < 0) throw new Error(`markers not found: ${startMarker}`);
  return preview.slice(a, b).trim();
}

function stripAlgo(html) {
  return html.replace(/<details class="algo-legend">[\s\S]*?<\/details>\s*/g, "");
}

let reactor = sliceBetween('<section id="sectionReactor"', "<!-- ═══ WATCH PARTY LIVE ═══ -->");
reactor = stripAlgo(reactor);
reactor = reactor.replace(
  /<!-- LOADING STATE -->[\s\S]*?(?=\s*<button type="button" class="fab-compose")/m,
  ""
);
reactor = reactor.replace(
  '<div class="feed-panel active" data-panel="for-you">',
  `<div class="feed-panel active" data-panel="for-you">
          <h3 class="reactor-discussions-heading" id="communityGeneralHeading">Show discussions</h3>
          <p class="reactor-discussions-err err" id="communityThreadsErr" style="display: none"></p>
          <ul class="community-general-list social-reactor-threads" id="communityGeneralList" aria-busy="true"></ul>`
);
reactor = reactor.replace(
  '<button type="button" class="fab-compose" id="fabCompose"',
  '<button type="button" class="fab-compose hidden" id="fabCompose" hidden'
);

const nav = sliceBetween('<nav class="social-top-nav"', '<section id="sectionReactor"');
const watch = stripAlgo(sliceBetween("<!-- ═══ WATCH PARTY LIVE ═══ -->", "<!-- ═══ SHOW DIARY ═══ -->"));
const diary = stripAlgo(sliceBetween("<!-- ═══ SHOW DIARY ═══ -->", "<!-- ═══ BREAKING NEWS"));

const news = `<section id="sectionNews" class="social-section" aria-label="Breaking News">
      <div class="content-scroll" id="newsScroll">
        <p class="community-replace-banner">
          <strong>Gossip hub.</strong> Your shows, cast drama, and TV tea — pulled from your watchlist.
        </p>
        <div class="gossip-page-head">
          <h2>Today's Tea &#9749;</h2>
          <p id="socialGossipMeta">Loading stories…</p>
        </div>
        <p class="gossip-empty err" id="socialGossipErr" hidden role="alert"></p>
        <div id="socialGossipLoading" aria-hidden="true">
          <div class="gossip-skeleton-hero"></div>
          <div class="gossip-skeleton-item"></div>
          <div class="gossip-skeleton-item"></div>
          <div class="gossip-skeleton-item"></div>
        </div>
        <div id="socialGossipContent" hidden>
          <div id="socialGossipHeroWrap"></div>
          <div class="gossip-trending" id="socialGossipTrending" aria-label="Trending topics" hidden></div>
          <h3 class="gossip-section-label">Latest headlines <span>From your shows</span></h3>
          <ul class="gossip-feed" id="socialGossipFeed"></ul>
          <div id="socialGossipIndustryBlock">
          <div class="gossip-divider"></div>
          <h3 class="gossip-section-label">Industry &amp; casting <span>Broader TV news</span></h3>
          <ul class="gossip-feed" id="socialGossipIndustryFeed"></ul>
          </div>
        </div>
        <p class="gossip-empty" id="socialGossipEmpty" hidden>No stories in the last 48 hours. Check back soon.</p>
      </div>
    </section>`;

let overlays = preview.slice(preview.indexOf("<!-- Live chat overlay -->"), preview.indexOf("<!-- Composer -->")).trim();
overlays = overlays
  .replace(/id="liveChatOverlay"/g, 'id="socialLiveChatOverlay"')
  .replace(/id="diaryDetailOverlay"/g, 'id="socialDiaryDetailOverlay"')
  .replace(/id="joinLiveChat"/g, 'id="socialJoinLiveChat"')
  .replace(/id="viewReplay"/g, 'id="socialViewReplay"')
  .replace(/id="exitLiveChat"/g, 'id="socialExitLiveChat"')
  .replace(/id="closeDiaryDetail"/g, 'id="socialCloseDiaryDetail"');

const listHtml = `<div class="community-list-view social-hub" id="communityListView" data-social-section="reactor">
${nav}
${reactor}
${watch}
${diary}
${news}
${overlays}
</div>`;

fs.writeFileSync(path.join(root, "public/social-hub-list.html"), listHtml);
console.log("ok", listHtml.length);
