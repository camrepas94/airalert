#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const indexPath = path.join(root, "public/index.html");
let html = fs.readFileSync(indexPath, "utf8");

const listHtml = fs.readFileSync(path.join(root, "public/social-hub-list.html"), "utf8");
const indented = listHtml
  .split("\n")
  .map((line) => (line.trim() ? "            " + line : line))
  .join("\n");

const listStart = html.indexOf('          <div class="community-list-view" id="communityListView">');
const start = listStart;
const threadStart = html.indexOf('          <div class="community-thread-view hidden" id="communityThreadView">');
if (start < 0 || threadStart < 0) throw new Error("template markers not found");

html = html.slice(0, start) + indented + "\n" + html.slice(threadStart);

if (!html.includes('href="/social-hub.css"')) {
  html = html.replace(
    '<link rel="stylesheet" href="/legal/legal-doc-prose.css" />',
    '<link rel="stylesheet" href="/legal/legal-doc-prose.css" />\n    <link rel="stylesheet" href="/social-hub.css" />',
  );
}

html = html.replace(
  'id="communityBackBtn">← Threads</button>',
  'id="communityBackBtn">← Back</button>',
);

html = html.replace(
  `#tab-community .community-list-view {
          padding-left: max(0.75rem, env(safe-area-inset-left, 0px));
          padding-right: max(0.75rem, env(safe-area-inset-right, 0px));
          box-sizing: border-box;
        }`,
  `#tab-community .community-list-view.social-hub {
          padding-left: 0;
          padding-right: 0;
          box-sizing: border-box;
        }`,
);

const socialHubJs = `
      /** Social hub (Community tab list view): section nav + gossip digest + preview overlays. */
      let socialHubWired = false;
      let socialGossipPaintId = 0;

      function setSocialHubSection(key) {
        communityState.socialSection = key;
        const root = document.getElementById("communityListView");
        if (root) root.setAttribute("data-social-section", key);
      }

      function bindSocialHubSubTabs(sectionEl, tabAttr, panelAttr, panelClass) {
        if (!sectionEl) return;
        const tabs = sectionEl.querySelectorAll(".feed-tab");
        const panels = sectionEl.querySelectorAll("." + panelClass);
        tabs.forEach((tab) => {
          tab.addEventListener("click", () => {
            const val = tab.dataset[tabAttr];
            tabs.forEach((t) => {
              t.classList.toggle("active", t === tab);
              t.setAttribute("aria-selected", t === tab ? "true" : "false");
            });
            panels.forEach((p) => p.classList.toggle("active", p.dataset[panelAttr] === val));
          });
        });
      }

      function initSocialHub() {
        const listView = document.getElementById("communityListView");
        if (!listView || !listView.classList.contains("social-hub")) return;
        if (socialHubWired) return;
        socialHubWired = true;

        const sections = {
          reactor: document.getElementById("sectionReactor"),
          watch: document.getElementById("sectionWatch"),
          diary: document.getElementById("sectionDiary"),
          news: document.getElementById("sectionNews"),
        };

        listView.querySelectorAll(".social-nav-item").forEach((btn) => {
          btn.addEventListener("click", () => {
            const key = btn.dataset.section;
            if (!key) return;
            listView.querySelectorAll(".social-nav-item").forEach((b) => b.classList.toggle("active", b === btn));
            Object.entries(sections).forEach(([k, el]) => {
              if (el) el.classList.toggle("active", k === key);
            });
            setSocialHubSection(key);
            if (key === "news") void loadSocialHubGossip();
          });
        });

        bindSocialHubSubTabs(sections.reactor, "feed", "panel", "feed-panel");
        bindSocialHubSubTabs(sections.watch, "wp", "wpPanel", "wp-panel");
        bindSocialHubSubTabs(sections.diary, "diary", "diaryPanel", "diary-panel");

        listView.querySelectorAll(".reaction-btn:not(.bookmark)").forEach((btn) => {
          btn.addEventListener("click", () => btn.classList.toggle("selected"));
        });
        listView.querySelectorAll(".reaction-btn.bookmark").forEach((btn) => {
          btn.addEventListener("click", () => btn.classList.toggle("selected"));
        });
        listView.querySelectorAll(".read-more").forEach((btn) => {
          btn.addEventListener("click", () => {
            const el = document.getElementById(btn.dataset.target);
            if (el) {
              el.classList.remove("collapsed");
              btn.hidden = true;
            }
          });
        });

        const spoilerOverlay = document.getElementById("spoilerOverlay");
        if (spoilerOverlay) {
          spoilerOverlay.addEventListener("click", () => {
            spoilerOverlay.classList.add("revealed");
            document.getElementById("spoilerContent")?.classList.remove("post-content-hidden");
          });
        }

        const liveChat = document.getElementById("socialLiveChatOverlay");
        const replayBar = document.getElementById("replayBar");
        const highlightVote = liveChat?.querySelector(".highlight-vote");
        function openSocialLiveChat(replay) {
          liveChat?.classList.add("open");
          if (replayBar) replayBar.hidden = !replay;
          if (highlightVote) highlightVote.hidden = !!replay;
        }
        function closeSocialLiveChat() {
          liveChat?.classList.remove("open");
        }
        document.getElementById("socialJoinLiveChat")?.addEventListener("click", () => openSocialLiveChat(false));
        listView.querySelectorAll(".join-live-btn").forEach((b) =>
          b.addEventListener("click", () => openSocialLiveChat(false)),
        );
        document.getElementById("socialViewReplay")?.addEventListener("click", () => openSocialLiveChat(true));
        document.getElementById("socialExitLiveChat")?.addEventListener("click", closeSocialLiveChat);

        const chatMessages = liveChat?.querySelector("#chatMessages");
        const jumpLatest = liveChat?.querySelector("#jumpLatest");
        chatMessages?.addEventListener("scroll", () => {
          if (!jumpLatest || !chatMessages) return;
          const atBottom = chatMessages.scrollHeight - chatMessages.scrollTop - chatMessages.clientHeight < 80;
          jumpLatest.classList.toggle("visible", !atBottom);
        });
        jumpLatest?.addEventListener("click", () => {
          if (!chatMessages) return;
          chatMessages.scrollTop = chatMessages.scrollHeight;
          jumpLatest.classList.remove("visible");
        });

        const diaryOverlay = document.getElementById("socialDiaryDetailOverlay");
        listView.querySelectorAll(".diary-day").forEach((day) => {
          day.addEventListener("click", () => diaryOverlay?.classList.add("open"));
        });
        document.getElementById("socialCloseDiaryDetail")?.addEventListener("click", () =>
          diaryOverlay?.classList.remove("open"),
        );

        const ptr = document.getElementById("ptrIndicator");
        const scroll = document.getElementById("feedScroll");
        let ptrStart = 0;
        scroll?.addEventListener("touchstart", (e) => {
          ptrStart = e.touches[0].clientY;
        }, { passive: true });
        scroll?.addEventListener("touchmove", (e) => {
          if (scroll.scrollTop === 0 && e.touches[0].clientY > ptrStart + 60) ptr?.classList.add("visible");
        }, { passive: true });
        scroll?.addEventListener("touchend", () => {
          if (ptr?.classList.contains("visible")) setTimeout(() => ptr.classList.remove("visible"), 1200);
        });

        setSocialHubSection(communityState.socialSection || "reactor");
      }

      function gossipTrendTag(showName) {
        if (!showName || !String(showName).trim()) return "";
        const compact = String(showName)
          .replace(/[^a-zA-Z0-9]+/g, "")
          .slice(0, 24);
        return compact ? "#" + compact : "";
      }

      function renderSocialGossipHero(it, posterUrl) {
        const wrap = document.getElementById("socialGossipHeroWrap");
        if (!wrap) return;
        const hasUrl = Boolean(it.url && String(it.url).trim());
        const el = document.createElement(hasUrl ? "a" : "div");
        el.className = "gossip-hero";
        if (hasUrl) {
          el.href = it.url;
          el.target = "_blank";
          el.rel = "noopener";
        }
        const headlineText = decodeHtmlEntities(it.text || "");
        const showName = decodeHtmlEntities(it.showName || "");
        const source = decodeHtmlEntities(it.source || "");
        const timeLabel = it.publishedAt ? formatCommunityRelativeTime(it.publishedAt) : "";
        const chip =
          it.type === "breaking"
            ? "🔴 BREAKING"
            : it.type === "airing"
              ? "📺 TONIGHT"
              : headlineBadgeLabel(it.type).toUpperCase();
        el.innerHTML =
          '<div class="gossip-hero-bg"></div>' +
          '<div class="gossip-hero-scrim"></div>' +
          '<div class="gossip-hero-body">' +
          '<span class="gossip-hero-chip">' +
          escapeHtml(chip) +
          "</span>" +
          (showName ? '<span class="gossip-hero-show">' + escapeHtml(showName) + "</span>" : "") +
          '<h3 class="gossip-hero-title">' +
          escapeHtml(headlineText) +
          "</h3>" +
          '<p class="gossip-hero-meta">' +
          (source ? escapeHtml(source) : "") +
          (source && timeLabel ? " · " : "") +
          escapeHtml(timeLabel) +
          "</p></div>";
        const bg = el.querySelector(".gossip-hero-bg");
        if (bg && posterUrl) bg.style.backgroundImage = "url(" + posterUrl + ")";
        wrap.innerHTML = "";
        wrap.appendChild(el);
      }

      function renderSocialGossipFeedItem(it, posterUrl, opts) {
        const hasUrl = Boolean(it.url && String(it.url).trim());
        const li = document.createElement("li");
        const card = document.createElement(hasUrl ? "a" : "div");
        card.className = "gossip-item" + (opts && opts.hot ? " gossip-item--hot" : "");
        if (hasUrl) {
          card.href = it.url;
          card.target = "_blank";
          card.rel = "noopener";
        }
        const headlineText = decodeHtmlEntities(it.text || "");
        const showName = decodeHtmlEntities(it.showName || "");
        const source = decodeHtmlEntities(it.source || "");
        const timeLabel = it.publishedAt ? formatCommunityRelativeTime(it.publishedAt) : "";
        const badge =
          opts && opts.badge === "new"
            ? '<span class="gossip-item-badge gossip-item-badge--new">NEW</span>'
            : opts && opts.hot
              ? '<span class="gossip-item-badge">HOT</span>'
              : "";
        card.innerHTML =
          '<img class="gossip-thumb" alt="" loading="lazy" />' +
          '<div class="gossip-item-body">' +
          '<div class="gossip-item-top">' +
          (showName ? '<span class="gossip-item-show">' + escapeHtml(showName) + "</span>" : "") +
          badge +
          "</div>" +
          '<p class="gossip-item-title">' +
          escapeHtml(headlineText) +
          "</p>" +
          '<p class="gossip-item-meta">' +
          (source ? "<strong>" + escapeHtml(source) + "</strong>" : "") +
          (source && timeLabel ? " · " : "") +
          escapeHtml(timeLabel) +
          "</p></div>";
        const thumb = card.querySelector(".gossip-thumb");
        if (thumb && posterUrl) thumb.src = posterUrl;
        li.appendChild(card);
        return li;
      }

      async function loadSocialHubGossip() {
        const meta = document.getElementById("socialGossipMeta");
        const errEl = document.getElementById("socialGossipErr");
        const loading = document.getElementById("socialGossipLoading");
        const content = document.getElementById("socialGossipContent");
        const empty = document.getElementById("socialGossipEmpty");
        const feed = document.getElementById("socialGossipFeed");
        const industry = document.getElementById("socialGossipIndustryFeed");
        const trending = document.getElementById("socialGossipTrending");
        if (!meta || !feed) return;

        const paintId = ++socialGossipPaintId;
        if (errEl) {
          errEl.hidden = true;
          errEl.textContent = "";
        }
        if (empty) empty.hidden = true;
        if (content) content.hidden = true;
        if (loading) loading.hidden = false;
        if (meta) meta.textContent = "Loading stories…";

        try {
          const data = await api("/api/ticker");
          if (paintId !== socialGossipPaintId) return;
          const MS_48H = 48 * 60 * 60 * 1000;
          const items = (Array.isArray(data.items) ? data.items : []).filter((it) => {
            if (it.type !== "breaking") return true;
            const iso = it.publishedAt;
            if (!iso || typeof iso !== "string") return false;
            const ts = new Date(iso).getTime();
            return Number.isFinite(ts) && Date.now() - ts <= MS_48H;
          });

          if (!items.length) {
            if (loading) loading.hidden = true;
            if (empty) empty.hidden = false;
            if (meta) meta.textContent = "No recent stories";
            return;
          }

          const showItems = items.filter((it) => it.type === "breaking" || it.type === "airing");
          const industryItems = items.filter((it) => it.type === "admin" || it.type === "stat");
          const heroItem = showItems.find((it) => it.type === "breaking") || showItems[0] || items[0];
          const restShow = showItems.filter((it) => it !== heroItem);

          const posterMap = new Map();
          const ids = [...new Set(items.map((it) => it.tvmazeShowId).filter((id) => id != null))];
          await Promise.all(
            ids.map(async (id) => {
              const url = await fetchTvmazeShowImageUrl(id);
              if (url) posterMap.set(id, url);
            }),
          );
          if (paintId !== socialGossipPaintId) return;

          renderSocialGossipHero(heroItem, posterMap.get(heroItem.tvmazeShowId));

          if (trending) {
            const tags = [];
            for (const it of showItems) {
              const tag = gossipTrendTag(it.showName);
              if (tag && !tags.includes(tag)) tags.push(tag);
              if (tags.length >= 5) break;
            }
            trending.innerHTML = tags.map((t) => '<span class="gossip-trend-chip">' + escapeHtml(t) + "</span>").join("");
            trending.hidden = tags.length === 0;
          }

          feed.innerHTML = "";
          restShow.slice(0, 12).forEach((it, i) => {
            feed.appendChild(
              renderSocialGossipFeedItem(it, posterMap.get(it.tvmazeShowId), { hot: i === 0, badge: i === 1 ? "new" : null }),
            );
          });

          if (industry) {
            industry.innerHTML = "";
            industryItems.slice(0, 8).forEach((it) => {
              industry.appendChild(renderSocialGossipFeedItem(it, posterMap.get(it.tvmazeShowId)));
            });
            const industryBlock = document.getElementById("socialGossipIndustryBlock");
            if (industryBlock) industryBlock.hidden = industryItems.length === 0;
          }

          const count = showItems.length + industryItems.length;
          const updated = formatCommunityRelativeTime(new Date().toISOString());
          if (meta) meta.innerHTML = "Updated " + escapeHtml(updated) + "<br />" + count + " stor" + (count === 1 ? "y" : "ies");

          if (loading) loading.hidden = true;
          if (content) content.hidden = false;
        } catch (e) {
          if (paintId !== socialGossipPaintId) return;
          if (loading) loading.hidden = true;
          if (errEl) {
            errEl.textContent = e.message || "Couldn’t load stories.";
            errEl.hidden = false;
          }
          if (meta) meta.textContent = "Couldn’t load";
        }
      }
`;

const socialHubJsClean = socialHubJs;

const insertBefore = "      function scheduleCommunityListSecondary() {";
if (!html.includes("function initSocialHub()")) {
  if (!html.includes(insertBefore)) throw new Error("scheduleCommunityListSecondary not found");
  html = html.replace(insertBefore, socialHubJsClean + "\n" + insertBefore);
}

html = html.replace(
  `      function scheduleCommunityListSecondary() {
        void loadFriendsActivity();
        deferAfterIdle(() => {
          void loadCommunityHeadlines();
        }, 2200);
      }`,
  `      function scheduleCommunityListSecondary() {
        deferAfterIdle(() => {
          void loadSocialHubGossip();
        }, 800);
      }`,
);

html = html.replace(
  `        showNameFallback: "",
        sort: "newest",`,
  `        showNameFallback: "",
        socialSection: "reactor",
        sort: "newest",`,
);

html = html.replace(
  `      function initCommunityShellAfterMount() {
        relocateSocialFabLayers();
        initMentionAutocompleteDeferred();
        initShowSubtabsDeferred();
        setupCommunityCatchUpPanelDeferred();
        wireDeferredCommunityTabPanel();
        wireCommunityNewThreadFab();`,
  `      function initCommunityShellAfterMount() {
        relocateSocialFabLayers();
        initSocialHub();
        initMentionAutocompleteDeferred();
        initShowSubtabsDeferred();
        setupCommunityCatchUpPanelDeferred();
        wireDeferredCommunityTabPanel();
        wireCommunityNewThreadFab();`,
);

html = html.replace(
  `        void loadFriendsActivity();
        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });
      }

      function openCommunityListView() {`,
  `        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });
      }

      function openCommunityListView() {`,
);

html = html.replace(
  `        void loadFriendsActivity();
        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });
      }

      /* ── Community breaking news`,
  `        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });
      }

      /* ── Community breaking news`,
);

html = html.replace(
  `          if (communityState.view === "list") {
            void loadFriendsActivity({ force: true });
            void loadCommunityThreads().then(() => {
              scheduleCommunityListSecondary();
            });`,
  `          if (communityState.view === "list") {
            initSocialHub();
            void loadCommunityThreads().then(() => {
              scheduleCommunityListSecondary();
            });
            if (communityState.socialSection === "news") void loadSocialHubGossip();`,
);

html = html.replace(
  `        if (lv) lv.classList.remove("hidden");
        if (tv) tv.classList.add("hidden");
        void loadFriendsActivity();
        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });`,
  `        if (lv) lv.classList.remove("hidden");
        if (tv) tv.classList.add("hidden");
        setSocialHubSection(communityState.socialSection || "reactor");
        void loadCommunityThreads().then(() => {
          scheduleCommunityListSecondary();
        });`,
);

fs.writeFileSync(indexPath, html);
console.log("patched index.html");
