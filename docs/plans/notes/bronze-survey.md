# Bronze survey (S1)

Surveyed 2026-11-02 over `glue.bsky_raw.jetstream` with DuckDB reading the Iceberg table directly (`silver/scripts/`). Per-post numbers are for **2026-11-01**, the first complete UTC day. The collection mix also includes the first 11 hours of 2026-11-02. The fixtures are in `silver/tests/fixtures/jetstream.jsonl`.

## Volume (2026-11-01)

| Kind | Events |
|---|---|
| commit | 34,316,966 |
| account | 34,250 |
| identity | 27,997 |
| **total** | **34,379,213** (~400/s) |

**Posts per day:** 3,358,242 creates, 299,949 deletes and 3,219 updates. Creates are the input to the C2 cost estimate. 96% of created posts have text; the mean is 94 characters.

## Collection mix (both days, 47.0M events)

| Collection | Operation | Share |
|---|---|---|
| app.bsky.feed.like | create | 66.24% |
| app.bsky.feed.repost | create | 10.20% |
| app.bsky.feed.post | create | 9.79% |
| app.bsky.graph.follow | create | 6.21% |
| app.bsky.graph.block | create | 1.31% |
| app.bsky.feed.like | delete | 1.18% |
| app.bsky.graph.follow | delete | 0.95% |
| app.bsky.feed.post | delete | 0.86% |
| app.bsky.feed.threadgate | create | 0.58% |
| app.bsky.feed.repost | delete | 0.42% |
| app.bsky.feed.postgate | create | 0.34% |
| app.bsky.actor.profile | update | 0.29% |
| app.bsky.graph.listitem | create | 0.25% |
| account | | 0.10% |
| identity | | 0.08% |
| app.bsky.actor.profile | create | 0.06% |

- **Collections above 0.1%:** 9 of the 1,037 distinct collections seen on Nov 1. They are like, repost, post, follow, block, threadgate, postgate, profile and listitem.
- **Non-`app.bsky` collections:** 0.76% of commits, spread over about 1,000 collections. The largest namespaces are `place.stream`, `app.rocksky`, `org.titlegraph` and `fm.teal`. Silver ignores them and counts them.

## Posts: optional fields (Nov 1 creates)

| Field | Share of posts |
|---|---|
| `reply` | 45.8% (57% of these reply directly to the thread root) |
| `embed` | 37.7% |
| `facets` | 22.0% |
| `langs`, non-empty | 89.1% (10.7% have no field, 0.2% an empty list, 0.35% several langs) |
| self-`labels` | 1.0% |
| empty text | 4.0% |

**Embed types (share of all posts):**
- `app.bsky.embed.images`: 15.32%. 26% of image posts have alt text on at least one image, and there are 1.23 images per post.
- `app.bsky.embed.external`: 13.07%.
- `app.bsky.embed.record` (quote): 5.99%.
- `app.bsky.embed.video`: 2.05%.
- `app.bsky.embed.recordWithMedia`: 1.01%. Its media is images (19,192), external (12,177), video (2,074) or gallery (407).
- **`app.bsky.embed.gallery`: 0.30%. This type isn't in the S2 plan.**
- About 70 posts carry nonstandard types, such as `app.midnightsky.post` and `app.bsky.embed.images#main`.

**Facet features (share of posts with at least one):**
- `#link`: 12.64%.
- `#tag`: 10.43%.
- `#mention`: 1.83%.
- About 1,000 posts carry nonstandard features, such as `com.objkt.richtext.facet#token`, `app.bsky.richtext.facet#hashtag` and `blue.moji.richtext.facet`.

**Languages (first entry):** `en` 59.1%, `ja` 11.7%, none 10.9%, `de` 3.1%, `es` 2.9%, `pt` 2.7%, `fr` 1.8%, `ko` 1.2%, `ne` 1.1% (mostly a number-counting bot), `nl` 0.9%.

**Self-label values:**
- `porn` (18,583), `sexual` (4,865), `graphic-media` (2,498), `nudity` (2,358), `bot` (314) and `!no-unauthenticated` (200).
- Then a long tail of freeform values used as tags, such as `tag-apple` and `store-ebay`.

**Text length:** median 63 characters, p90 245, p99 300. The maximum is 1,460: the 300 limit counts graphemes, and some clients don't enforce it.

**`createdAt` compared with the event time (`time_us`):**
- Median −0.9 s; p01 −24 min; p99 +6.5 s.
- 0.20% of posts are more than 5 minutes in the future.
- 0.17% are more than a day old (backdated or imported).
- None are unparseable.

**Duplicates:** none among Nov 1 post creates by `(did, rkey, rev)`. No restarts happened that day; restarts still replay a 5 s overlap, so dedup remains necessary.

## Profiles, identity and account

- **Profiles (Nov 1 creates and updates, 119,114):** `displayName` 97%, non-empty `description` 60%, self-labels 15%.
- **Account events (both days, 47,662):**
  - 33,435 have `active=true` and no status.
  - The rest have `active=false`, with status `takendown` (4,346), `deleted` (3,506), `throttled` (3,481) or `deactivated` (2,894).
- **Identity events:** 38,351.

## Implications for S2

- **Embeds:** add `app.bsky.embed.gallery`, both top-level and as `recordWithMedia` media. Its alt text is at `items[*].alt` (items of type `app.bsky.embed.gallery#image`), not `images[*].alt`, so `image_alts` has to read both. Keep the raw `$type` in `embed_type` so the nonstandard tail survives.
- **Facets:** extract only the three `app.bsky.richtext.facet` features and ignore others. The `record` column keeps the rest.
- **`langs`:** a missing field and an empty list both mean unknown. Store an empty list for both.
- **Text:** don't assume 300 characters.
- **`created_at`:** can't be trusted for partitioning. Partition by event time (`time_us`/`received_at`) and keep `created_at` as data.
- **Account status:** includes `throttled`, so keep it a free string rather than an enum.
- **No silver table yet:** threadgate, postgate and listitem are above 0.1%. They are counted like unknown collections until a use appears.
- **C1 state:**
  - Alt text is sparse, at 26% of image posts.
  - 11% of posts have no language.
  - Self-labels already mark some adult content (about 0.8% of posts), which is useful as a weak label for the `adult` question.

## Fixtures

`silver/tests/fixtures/jetstream.jsonl` holds 184 lines of `{"case": ..., "event": <raw Jetstream event>}` in 44 cases:
- every collection above 0.1%, with creates and deletes;
- every post shape listed above, including gallery, nonstandard embeds and facets, no or several langs, empty and long text, backdated posts, updates and deletes;
- profile creates, updates and deletes;
- identity events, and account events for every status;
- non-`app.bsky` collections.

`silver/scripts/bronze_fixtures.py` makes the sample deterministically. Posts and profiles with adult self-labels, explicit or crude keywords, or text that looks like an email address or phone number are excluded.
