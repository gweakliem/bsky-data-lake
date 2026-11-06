---
title: Pipeline health
toc: false
---

```js
const freshness = FileAttachment("data/freshness.json").json();
const runs = FileAttachment("data/runs.csv").csv({typed: true});
const appended = FileAttachment("data/appended.csv").csv({typed: true});
const rejects = FileAttachment("data/rejects.csv").csv({typed: true});
```

```js
// Reference palette (see the dataviz notes in reports/CLAUDE.md): categorical slots 1-4, stepped per mode.
const series = dark ? ["#3987e5", "#d95926", "#199e70", "#c98500"] : ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"];
const status = {good: "#0ca30c", warning: "#fab219", critical: "#d03b3b"};
const minutes = (a, b) => (new Date(a) - new Date(b)) / 60000;
const ago = (m) => (m < 90 ? `${Math.round(m)} min` : `${(m / 60).toFixed(1)} h`);
const utc = d3.utcFormat("%b %-d, %H:%M UTC");
```

# Pipeline health

<p class="muted">Built ${utc(new Date(freshness.built_at))}, ${ago(minutes(now, freshness.built_at))} ago. Figures are as of the build; the site rebuilds hourly.</p>

```js
const bronzeAge = freshness.bronze_ingested_at && minutes(freshness.built_at, freshness.bronze_ingested_at);
const silverLag = freshness.bronze_ingested_at && minutes(freshness.bronze_ingested_at, freshness.silver_bronze_through);

function tile({title, value, detail, state}) {
  const s = {good: ["●", "OK"], warning: ["▲", "Behind"], critical: ["■", "Stale"]}[state];
  return html`<div class="card tile">
    <h2>${title}</h2>
    <span class="big">${value}</span>
    <div class="status"><span style="color: ${status[state]}">${s[0]}</span> ${s[1]}</div>
    <div class="muted">${detail}</div>
  </div>`;
}
```

<div class="grid grid-cols-3">
  ${tile({
    title: "Bronze freshness",
    value: bronzeAge == null ? "none in 2 days" : ago(bronzeAge),
    detail: freshness.bronze_ingested_at ? `max(ingested_at) ${utc(new Date(freshness.bronze_ingested_at))}` : "no bronze rows since yesterday",
    state: bronzeAge == null ? "critical" : bronzeAge > 15 ? "warning" : "good"
  })}
  ${tile({
    title: "Silver behind bronze",
    value: silverLag == null ? "–" : ago(Math.max(0, silverLag)),
    detail: `silver through ${utc(new Date(freshness.silver_bronze_through))}`,
    state: silverLag == null ? "critical" : silverLag > 90 ? "warning" : "good"
  })}
  ${tile({
    title: "Last silver window",
    value: ago(minutes(freshness.built_at, freshness.silver_finished_at)) + " ago",
    detail: `finished ${utc(new Date(freshness.silver_finished_at))}`,
    state: minutes(freshness.built_at, freshness.silver_finished_at) > 90 ? "warning" : "good"
  })}
</div>

<p class="muted">Bronze is behind when its newest row is over 15 minutes old, as for the <code>bsky-ingest-stalled</code> alarm. Silver runs hourly at :05, so up to about an hour behind bronze is normal.</p>

## Silver, last 7 days

<div class="grid grid-cols-2">
  <div class="card">${resize((width) => Plot.plot({
    title: "Bronze rows processed per hour",
    width,
    height: 260,
    x: {type: "utc", label: null},
    y: {grid: true, label: "rows", tickFormat: "s"},
    marks: [
      Plot.lineY(runs, Plot.binX({y: "sum", filter: null}, {x: "finished_at", y: "bronze_rows", interval: "hour", stroke: series[0], strokeWidth: 2, curve: "step", tip: {format: {y: ",d"}}})),
      Plot.ruleY([0])
    ]
  }))}</div>
  <div class="card">${resize((width) => Plot.plot({
    title: "How far each window trailed its run (minutes, log scale)",
    width,
    height: 260,
    x: {type: "utc", label: null},
    y: {type: "log", grid: true, label: "minutes", ticks: [1, 10, 60, 600, 6000], tickFormat: ","},
    marks: [
      Plot.ruleY([60], {stroke: "currentColor", strokeOpacity: 0.4, strokeDasharray: "4,3"}),
      Plot.text([60], {y: (d) => d, frameAnchor: "right", dy: -6, text: () => "1 h", fill: "currentColor", fillOpacity: 0.6}),
      Plot.dot(runs.filter((d) => d.lag_minutes > 0), {x: "finished_at", y: "lag_minutes", r: 3, fill: series[0], stroke: "var(--theme-background)", strokeWidth: 1, tip: {channels: {rows: "bronze_rows", seconds: "seconds"}, format: {rows: ",d"}}})
    ]
  }))}</div>
</div>

<p class="muted">Lag is a window's <code>finished_at</code> minus its <code>bronze_through</code>. A run's first windows trail most; during a backlog it grows to hours or days until silver catches up, hence the log scale.</p>

```js
// Fixed groups, so a table keeps its color from build to build: the three largest
// tables, and the rest folded into "other" (the table below has every table).
const groups = ["likes", "post_edges", "reposts", "other"];
const folded = appended.map((d) => ({...d, group: groups.includes(d.table) ? d.table : "other"}));
const totals = d3.rollups(appended, (v) => d3.sum(v, (d) => d.rows), (d) => d.table).sort((a, b) => b[1] - a[1]);
```

<div class="card">${resize((width) => Plot.plot({
  title: "Rows appended per hour, by silver table",
  width,
  height: 300,
  x: {type: "utc", label: null},
  y: {grid: true, label: "rows", tickFormat: "s"},
  color: {domain: groups, range: series, legend: true},
  marks: [
    Plot.rectY(folded, Plot.stackY(Plot.groupX({y: "sum"}, {x: "hour", y: "rows", fill: "group", interval: "hour", insetLeft: 1, insetRight: 1, stroke: "var(--theme-background)", strokeWidth: 1, tip: {format: {y: ",d"}}}))),
    Plot.ruleY([0])
  ]
}))}</div>

```js
Inputs.table(totals.map(([table, rows]) => ({table, rows})), {
  columns: ["table", "rows"],
  header: {rows: "rows appended, 7 days"},
  format: {rows: d3.format(",d")},
  sort: "rows",
  reverse: true,
  rows: 12
})
```

## Rejects, last 30 days

```js
rejects.length === 0
  ? html`<p>No rejected bronze rows in the last 30 days.</p>`
  : html`<div class="grid grid-cols-2">
      <div class="card">${resize((width) => Plot.plot({
        title: "Rejected rows per day",
        width,
        height: 220,
        x: {type: "utc", label: null},
        y: {grid: true, label: "rows"},
        marks: [
          Plot.rectY(rejects, Plot.groupX({y: "sum"}, {x: "day", y: "rows", interval: "day", fill: series[0], insetLeft: 1, insetRight: 1, tip: true})),
          Plot.ruleY([0])
        ]
      }))}</div>
      <div class="card">${Inputs.table(
        d3.rollups(rejects, (v) => d3.sum(v, (d) => d.rows), (d) => d.reason).map(([reason, rows]) => ({reason, rows})),
        {sort: "rows", reverse: true, rows: 8}
      )}</div>
    </div>`
```

<style>
.tile .big { font-size: 2rem; font-weight: 600; line-height: 1.2; }
.tile .status { margin: 0.25rem 0; }
.muted { color: var(--theme-foreground-muted); }
</style>
