---
title: Languages
toc: false
---

```js
const languages = FileAttachment("data/languages.csv").csv({typed: true});
```

```js
const series = dark ? ["#3987e5"] : ["#2a78d6"];
const fmt = d3.format(",d");
const pct = d3.format(".1%");
const day = d3.utcFormat("%b %-d");
const allDays = d3.sort(new Set(languages.map((d) => +d.day))).map((t) => new Date(t));
```

# Languages

<p class="muted">Languages that current posts declare (<code>langs</code>), per day received (UTC). A post counts once for each language it declares, by primary subtag (<code>en-US</code> counts as <code>en</code>); posts that declare none are <code>(none)</code>, and languages outside the window's top 12 are <code>(other)</code>. Percentages are shares of language tags, not of posts.</p>

```js
const last = view(Inputs.range([1, allDays.length], {label: "Last N days", step: 1, value: Math.min(7, allDays.length)}));
```

```js
const from = allDays[allDays.length - last];
const inRange = languages.filter((d) => d.day >= from);
const totals = d3.sort(d3.rollups(inRange, (v) => d3.sum(v, (d) => d.posts), (d) => d.lang), ([, n]) => -n);
const total = d3.sum(totals, ([, n]) => n);
```

<div class="grid grid-cols-2">
  <div class="card">${resize((width) => Plot.plot({
    title: `Posts per language, ${day(from)} to ${day(allDays.at(-1))}`,
    width,
    height: 28 * totals.length + 40,
    marginLeft: 70,
    x: {grid: true, label: "posts", tickFormat: "s"},
    y: {label: null, domain: totals.map(([lang]) => lang)},
    marks: [
      Plot.barX(totals, {x: ([, n]) => n, y: ([lang]) => lang, fill: series[0], insetTop: 2, insetBottom: 2, rx: 2,
        tip: {format: {x: ",d"}}, channels: {share: ([, n]) => pct(n / total)}}),
      Plot.text(totals, {x: ([, n]) => n, y: ([lang]) => lang, text: ([, n]) => pct(n / total), dx: 4, textAnchor: "start", fill: "currentColor"}),
      Plot.ruleX([0])
    ]
  }))}</div>
  <div class="card">

```js
const lang = view(Inputs.select(totals.map(([l]) => l), {label: "Language"}));
```

```js
display(resize((width) => Plot.plot({
  title: `Share of language tags that are ${lang}, per day`,
  width,
  height: 240,
  x: {type: "utc", label: null},
  y: {grid: true, label: null, tickFormat: "%", zero: true},
  marks: [
    Plot.lineY(
      allDays.filter((d) => d >= from).map((d) => {
        const ofDay = inRange.filter((r) => +r.day === +d);
        return {day: d, share: d3.sum(ofDay.filter((r) => r.lang === lang), (r) => r.posts) / d3.sum(ofDay, (r) => r.posts)};
      }),
      {x: "day", y: "share", stroke: series[0], strokeWidth: 2, marker: "circle", tip: {format: {y: ".1%", x: "%b %-d"}}}
    )
  ]
})));
```

  </div>
</div>

```js
Inputs.table(
  d3.sort(d3.groups(inRange, (d) => +d.day), ([d]) => -d).map(([d, rows]) => ({
    day: new Date(d),
    ...Object.fromEntries(totals.map(([l]) => [l, rows.find((r) => r.lang === l)?.posts ?? 0]))
  })),
  {format: {day: d3.utcFormat("%Y-%m-%d"), ...Object.fromEntries(totals.map(([l]) => [l, fmt]))}, rows: 15}
)
```

<style>
.muted { color: var(--theme-foreground-muted); }
</style>
