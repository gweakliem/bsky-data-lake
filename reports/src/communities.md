---
title: Communities
toc: false
---

```js
const snap = FileAttachment("data/communities.json").json();
```

```js
const fmt = d3.format(",d");
const pct = d3.format(".0%");
const x1 = d3.format(".1f");
const langName = new Intl.DisplayNames(["en"], {type: "language"});
const lang = (l) => { try { return l === "?" ? "unknown" : langName.of(l); } catch { return l; } };
```

# Communities

```js
snap.available ? html`<p class="muted">Groups of accounts that like the same posts, from ${fmt(snap.likes)} likes between <b>${snap.start}</b> and <b>${snap.end}</b> by ${fmt(snap.accounts)} active accounts. Nothing here is about who follows whom or what anyone says about themselves: two accounts are close when they like the same posts, especially posts few others like. The words come from the posts each community likes far more than everyone else does. ${snap.adult_left_out} adult communities are left out. No accounts are listed; communities are aggregates.</p>` : html`<p class="muted">No clustering snapshot has been published yet.</p>`
```

```js
const communities = snap.available ? snap.communities : [];
const search = view(Inputs.search(communities, {placeholder: "Search communities by word…", columns: ["words"]}));
```

```js
display(html`<div class="grid grid-cols-3">${search.map((c) => html`<div class="card community" id="community-${c.id}">
  <h2>${c.words.slice(0, 4).join(", ")}</h2>
  <p class="stats">${fmt(c.accounts)} accounts · ${c.languages.filter(([, v]) => v >= 0.01).map(([l, v]) => `${lang(l)} ${pct(v)}`).join(", ")}</p>
  <p class="muted small">${c.words.join(" · ")}</p>
  ${c.stories?.length ? html`<p class="small"><b>Stories it likes most:</b> ${c.stories.map((s) => html`<a href="./stories#story-${s.id}">${s.words.slice(0, 3).join(", ")}</a> ${x1(s.lift)}×`).reduce((a, b) => html`${a}, ${b}`)}</p>` : ""}
  <details><summary>Posts it likes far more than everyone else</summary><ul>${c.examples.map((e) => html`<li><a href="${e.url}" target="_blank" rel="noopener">${e.text}</a></li>`)}</ul></details>
</div>`)}</div>`);
```

<style>
.muted { color: var(--theme-foreground-muted); }
.small { font-size: 0.85rem; }
.community h2 { margin-top: 0; font-size: 1.05rem; }
.community .stats { margin: 0.2rem 0; }
.community details { font-size: 0.85rem; }
.community li { margin-bottom: 0.3rem; }
</style>
