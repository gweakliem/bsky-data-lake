---
title: Stories
toc: false
---

```js
const snap = FileAttachment("data/stories.json").json();
```

```js
const series = dark ? "#3987e5" : "#2a78d6";
const fmt = d3.format(",d");
const pct = d3.format(".0%");
const x1 = d3.format(".1f");
const langName = new Intl.DisplayNames(["en"], {type: "language"});
const lang = (l) => { try { return l === "?" ? "unknown" : langName.of(l); } catch { return l; } };
const words = (s, n = 4) => s.words.slice(0, n).join(", ");
```

# Stories

```js
snap.available ? html`<p class="muted">What Bluesky talked about on <b>${snap.day}</b> (UTC), found by clustering the day's posts by meaning: ${fmt(snap.embedded_posts)} posts with text embeddings, of which ${fmt(snap.clustered_posts)} were clustered after leaving out replies, adult content, very high-volume accounts and copy-paste spam. A <b>story</b> is one cluster, or several merged because they're the same topic at the same time, often across languages. Stories are ranked by how many more posts they drew than yesterday's rate would predict. The words are the ones each story uses far more than other posts in its language; they're picked by the method, not written by a person.</p>` : html`<p class="muted">No clustering snapshot has been published yet.</p>`
```

```js
const stories = snap.available ? snap.stories : [];
const topics = snap.available ? snap.topics : [];
```

```js
function hourly(s, width) {
  const data = s.hourly.map((n, h) => ({h, n, burst: (h - s.burst_hour + 24) % 24 < 3}));
  return Plot.plot({
    width, height: 90, marginLeft: 36, marginBottom: 22,
    x: {type: "band", label: null, tickFormat: (h) => (h % 6 ? "" : `${String(h).padStart(2, "0")}Z`)},
    y: {label: null, tickFormat: "s", ticks: 2, grid: true},
    marks: [
      Plot.barY(data, {x: "h", y: "n", fill: series, fillOpacity: (d) => (d.burst ? 1 : 0.45), inset: 1, rx: 2,
        title: (d) => `${String(d.h).padStart(2, "0")}:00 UTC: ${fmt(d.n)} posts${d.burst ? " (busiest 3 hours)" : ""}`}),
      Plot.ruleY([0])
    ]
  });
}

function card(s) {
  return html`<div class="card story" id="story-${s.id}">
    <h2>${s.rank ? `${s.rank}. ` : ""}${words(s)}</h2>
    <p class="stats">${fmt(s.posts)} posts by ${fmt(s.authors)} accounts${s.rank ? html`, <b>${fmt(s.excess)}</b> more than yesterday's rate (${x1(s.growth)}× its share)` : ""} · ${fmt(s.likes ?? 0)} likes that day</p>
    <p class="muted small">${s.words.join(" · ")}</p>
    ${resize((width) => hourly(s, width))}
    <p class="small">Posts per hour (UTC); highlighted bars are the busiest three hours, ${x1(s.burst)}× the share its language's posts usually have then.</p>
    <p class="small"><b>Languages:</b> ${s.languages.filter(([, v]) => v >= 0.01).map(([l, v]) => `${lang(l)} ${pct(v)}`).join(", ")}${s.clusters.length > 1 ? html` · <b>merged from ${s.clusters.length} clusters</b> (${d3.rollups(s.clusters, (v) => v.length, (c) => c.lang).sort((a, b) => b[1] - a[1]).map(([l, n]) => `${lang(l)}${n > 1 ? ` ×${n}` : ""}`).join(", ")})` : ""}</p>
    ${s.titles.length ? html`<p class="small"><b>Most shared links:</b> ${s.titles.map(([t, n]) => `“${t}” (${fmt(n)})`).join(" · ")}</p>` : ""}
    ${s.communities?.length ? html`<p class="small"><b>Communities that like it most:</b> ${s.communities.map((c) => html`<a href="./communities#community-${c.id}">${c.words.slice(0, 3).join(", ")}</a> ${x1(c.lift)}×`).reduce((a, b) => html`${a}, ${b}`)}</p>` : ""}
    <details><summary>Example posts</summary><ul>${s.examples.map((e) => html`<li><a href="${e.url}" target="_blank" rel="noopener">${e.text}</a></li>`)}</ul></details>
  </div>`;
}
```

```js
stories.length ? Inputs.table(stories, {
  columns: ["rank", "words", "posts", "excess", "growth", "burst", "likes"],
  header: {words: "story", excess: "vs yesterday", growth: "growth", burst: "burst"},
  format: {words: (w) => w.slice(0, 5).join(", "), posts: fmt, excess: fmt, likes: fmt, growth: (v) => `${x1(v)}×`, burst: (v) => `${x1(v)}×`},
  width: {words: 360}, rows: 12, select: false
}) : ""
```

```js
display(html`<div class="grid grid-cols-2">${stories.map(card)}</div>`);
```

## Standing topics

```js
topics.length ? html`<p class="muted">The day's largest clusters that aren't news: they draw about as much as yesterday.</p>` : ""
```

```js
display(html`<div class="grid grid-cols-3">${topics.map(card)}</div>`);
```

<style>
.muted { color: var(--theme-foreground-muted); }
.small { font-size: 0.85rem; }
.story h2 { margin-top: 0; font-size: 1.1rem; }
.story .stats { margin: 0.2rem 0; }
.story details { font-size: 0.85rem; }
.story li { margin-bottom: 0.3rem; }
</style>
