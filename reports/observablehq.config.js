// Observable Framework: https://observablehq.com/framework/config
export default {
  title: "bsky data lake",
  root: "src",
  // Data loaders are Python (DuckDB over the lake, see lib/lake.py), run in this uv project.
  interpreters: {".py": ["uv", "run", "--quiet", "python"]},
  theme: ["air", "near-midnight"],
  pager: false,
  footer: "Built from the silver tables (and bronze freshness) by DuckDB over the Iceberg catalog.",
  pages: [{name: "Pipeline health", path: "/"}]
};
