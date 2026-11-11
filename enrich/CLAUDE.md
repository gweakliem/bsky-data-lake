# enrich

Classification of silver posts with TypeSafe's Jev (System One API): spec, evaluation set and eval (WP C1); the bulk job (C2) comes later. A uv project; run commands from this directory.

```bash
uv sync
uv run pytest                                  # no AWS or API access needed
eval "$(aws configure export-credentials --profile <profile> --format env)"; export AWS_REGION=us-west-2
uv run python -m bsky_enrich.sample 2026-11-09 # draw the eval sample once (about 2 min)
uv run python -m bsky_enrich.label             # hand-label it, in a real terminal; resumable
uv run python -m bsky_enrich.evaluate          # ask Jev about labeled posts not yet cached, then report
```

`TYPESAFE_API_KEY` and `BSKY_AWS_ACCOUNT_ID` come from the repo's `.env`.

- **Spec (`spec.py`):** `SPEC_VERSION`, the pinned `MODEL` (`jev-1.14.0`, not `jev-latest`: thresholds are measured against one version), the eligibility rule, the state builder and the questions. Changing any of them is a new spec version; answers are only comparable within one.
- **Questions:** Nouls `spam`, `adult`, `ai_generated` and `humor` (joke, sarcasm or snark: a tone, not a topic, since a joke about a trial is still about `crime_justice`); Choice `topic` over `TOPICS` with a companion Noul `topic_fits` instead of an `other` option (TypeSafe's guidance: `other` competes for probability). Each question names the state fields it judges by backticked path. The eval adds `topic_reversed`, the same Choice with the options reversed, because Jev can lean toward the first option.
- **State:** `post` (text, langs, image alt texts, link title and domain, quoted post's text), `author.display_name`, `reply_parent.text`, and `thread_root.text` when the root isn't the parent: many replies only make sense next to what the thread is about (referenced posts are looked up over the week before the post). Empty fields are left out. Self-labels are not sent: they're the comparison for `adult`. No handles: Jetstream's identity events mostly omit them (35 of 964 in an hour on 2026-11-10), so silver's `identities.handle` is nearly always null.
- **Eligibility:** posts with no link whose text, alt texts and link title total under 21 characters aren't classified (about 17% of new posts). `ELIGIBLE_SQL` is the same rule for DuckDB; a test keeps them in step.
- **Eval set (`eval/`):** `sample.jsonl` and `labels.jsonl` are committed (a label can name a second acceptable topic, `topic_alt`; the eval counts either as correct) and hold only URIs, strata and labels, since the repo is public. Post text and Jev's answers live in the git-ignored `eval/.cache/` (`posts.jsonl` from `sample`, rebuilt for the committed sample with `sample --refresh` after a state change; `answers-<spec>-<model>.jsonl` from `evaluate`). The sample is eligible English posts from one day, stratified (random, adult signal, high-volume accounts, widely shared links, many hashtags, long posts from prolific accounts), one post per account, so its rates aren't population rates. Draws leave out posts already deleted. The v1 sample predates that: 9 of its 300 posts are deleted (as of 2026-11-10), 8 of them before the draw. They stay in, labeled from the cached text, since C2 would classify a post that existed when it ran.
- **Incremental reads (`incremental.py`, for C2 and E2):** pyiceberg over Glue. A support table's watermark is its own max `bronze_ingested_at` (from file stats); posts are read one finished silver window (`silver_runs.bronze_through`) at a time, minus posts deleted in the same window, sorted. `chunks` never splits a run of equal watermarks, because the loader stamps a whole commit with one `ingested_at`. `append` retries commit conflicts with Glue's compactor. `testing.py` builds a fake silver namespace on a SQLite catalog for tests (needs the dev group).
- **Cost:** Jev bills input tokens only, $0.042 per million (2026-11). A request is about 1,700 tokens with the order check, mostly question text, so about $70 per million posts. Every eligible post (about 2.9M a day) would be about $200 a day; C2 has to sample, and shorter questions are the other lever.
