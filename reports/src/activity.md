---
title: Activity
toc: false
---

```js
const activity = FileAttachment("data/activity.csv").csv({typed: true});
```

```js
const series = dark ? ["#3987e5"] : ["#2a78d6"];
const today = d3.utcDay.floor(new Date(d3.max(activity, (d) => d.day)));
const complete = (d) => d.day < today;
const fmt = d3.format(",d");
const kinds = [
  {kind: "posts", title: "Posts", note: "current posts (latest version, deleted ones left out)"},
  {kind: "likes", title: "Likes", note: "every like created"},
  {kind: "reposts", title: "Reposts", note: "every repost created"}
];
const days = d3.utcDays(d3.min(activity, (d) => d.day), today);
const lastFull = days.at(-1);
const value = (kind, day) => activity.find((d) => d.kind === kind && +d.day === +day)?.rows ?? 0;
```

# Activity

<p class="muted">Per day received (UTC), over the last ${days.length + 1} days. ${utc(today)} is still in progress, so its bar is lighter. Day totals depend on the ingestor's uptime: a gap in ingestion is a dip here, not a drop in Bluesky activity.</p>

```js
const utc = d3.utcFormat("%b %-d");
```

<div class="grid grid-cols-3">
  ${kinds.map(({kind, title}) => {
    const last = value(kind, lastFull), before = value(kind, d3.utcDay.offset(lastFull, -1));
    const change = before ? (last - before) / before : null;
    return html`<div class="card">
      <h2>${title}, ${utc(lastFull)}</h2>
      <span class="big">${fmt(last)}</span>
      <div class="muted">${change == null ? "" : `${change >= 0 ? "+" : "−"}${Math.abs(change * 100).toFixed(1)}% on ${utc(d3.utcDay.offset(lastFull, -1))}`}</div>
    </div>`;
  })}
</div>

<div class="grid grid-cols-3">
  ${kinds.map(({kind, title, note}) => html`<div class="card">${resize((width) => Plot.plot({
    title: `${title} per day`,
    subtitle: note,
    width,
    height: 240,
    x: {type: "utc", label: null, ticks: d3.utcDay.every(2), tickFormat: "%b %-d"},
    y: {grid: true, label: null, tickFormat: "s"},
    marks: [
      Plot.rectY(activity.filter((d) => d.kind === kind), {
        x: "day", y: "rows", interval: "day", fill: series[0],
        fillOpacity: (d) => (complete(d) ? 1 : 0.4),
        insetLeft: 1, insetRight: 1, rx: 2,
        tip: {format: {y: ",d", x: "%b %-d", fillOpacity: false}}
      }),
      Plot.ruleY([0])
    ]
  }))}</div>`)}
</div>

```js
Inputs.table(
  d3.sort(d3.groups(activity, (d) => +d.day), ([day]) => -day).map(([day, rows]) => ({
    day: new Date(day),
    ...Object.fromEntries(kinds.map(({kind}) => [kind, rows.find((d) => d.kind === kind)?.rows ?? 0]))
  })),
  {format: {day: d3.utcFormat("%Y-%m-%d"), posts: fmt, likes: fmt, reposts: fmt}, rows: 15}
)
```

<style>
.big { font-size: 2rem; font-weight: 600; line-height: 1.2; }
.muted { color: var(--theme-foreground-muted); }
</style>
