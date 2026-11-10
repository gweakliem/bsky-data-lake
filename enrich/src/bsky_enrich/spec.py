"""Classification spec v1: which posts are classified, what Jev sees, what it's asked.

One System One request per post. Every question sees the same state and names
the fields it judges by path (TypeSafe's state guidance), so the state carries
only what some question needs. Changing a question, the state or the
eligibility rule is a new SPEC_VERSION: answers are only comparable within one.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from typesafe_sdk import Choice, Noul, NoulCriteria

SPEC_VERSION = "v1"
# Pinned, not jev-latest: the thresholds are measured against this version.
MODEL = "jev-1.14.0"

# Posts whose text, image alt texts and link title together are this short, and
# that have no link, aren't classified: too little to judge (about 17% of new
# posts on 2026-11-09). A short post with a link card still is.
MIN_CONTENT_CHARS = 21


def content_chars(post: dict[str, Any]) -> int:
    return (
        len(post.get("text") or "")
        + sum(len(a or "") for a in post.get("image_alts") or [])
        + len(post.get("external_title") or "")
    )


def eligible(post: dict[str, Any]) -> bool:
    return bool(post.get("external_uri")) or content_chars(post) >= MIN_CONTENT_CHARS


ELIGIBLE_SQL = (
    "(external_uri IS NOT NULL OR length(coalesce(text, '')) "
    "+ coalesce(list_sum(list_transform(image_alts, a -> length(coalesce(a, '')))), 0) "
    f"+ length(coalesce(external_title, '')) >= {MIN_CONTENT_CHARS})"
)


def _domain(uri: str | None) -> str | None:
    if not uri:
        return None
    host = urlsplit(uri).hostname or ""
    return host.removeprefix("www.") or None


def state(
    post: dict[str, Any],
    *,
    handle: str | None = None,
    display_name: str | None = None,
    quoted_text: str | None = None,
    parent_text: str | None = None,
    root_text: str | None = None,
) -> dict[str, Any]:
    """The state for one post. Empty fields are left out rather than sent as nulls.

    Self-labels are deliberately not sent: they're the comparison for `adult`.
    A reply's thread root is sent when it isn't also the parent: a reply often
    only makes sense next to what the thread is about.
    """
    p: dict[str, Any] = {"text": post.get("text") or ""}
    if post.get("langs"):
        p["langs"] = list(post["langs"])
    if post.get("image_alts"):
        p["image_alts"] = [a for a in post["image_alts"] if a]
    if post.get("external_uri"):
        p["link"] = {k: v for k, v in {"title": post.get("external_title"), "domain": _domain(post["external_uri"])}.items() if v}
    if quoted_text:
        p["quoted_post_text"] = quoted_text
    out: dict[str, Any] = {"post": p}
    author = {k: v for k, v in {"handle": handle, "display_name": display_name}.items() if v}
    if author:
        out["author"] = author
    if parent_text:
        out["reply_parent"] = {"text": parent_text}
    if root_text and root_text != parent_text:
        out["thread_root"] = {"text": root_text}
    return out


# Topics, in a fixed order. No "other": `topic_fits` says whether any applies
# (TypeSafe's companion-Noul pattern), and code ignores `topic` when it doesn't.
TOPICS: dict[str, str] = {
    "politics": "Elections, governments, politicians, parties, legislation, or political opinion",
    "social_issues": "Social policy and justice: rights, identity, immigration, health care, education, inequality",
    "news": "A current event that no other topic covers: disasters, accidents, weather, local news",
    "crime_justice": "Crime, police, courts, trials, lawyers and prisons",
    "sports": "Sports, athletes, teams, games and matches, fitness",
    "tech": "Technology, software, the internet, AI, gadgets, social media platforms",
    "science": "Science, research, medicine, nature, space, the environment and climate",
    "entertainment": "Movies, TV, music, books, games, celebrities, fandom, art and creative work",
    "business": "Business, companies, markets, the economy, jobs, money, products and selling",
    "personal": "The author's own life, feelings, day, pets, food, or chat with friends",
}

TOPIC_INSTRUCTIONS = (
    "What is `post` mainly about? Judge `post.text`, with `post.image_alts`, `post.link`, `post.quoted_post_text`, "
    "and for a reply `reply_parent` and `thread_root`, as context."
)

QUESTIONS: dict[str, Any] = {
    "spam": Noul(
        instructions="Is `post` spam? Judge `post.text` and `post.link`, with `author` as context.",
        criteria=NoulCriteria(
            true="Unsolicited promotion or advertising, a scam or phishing, follower or engagement bait "
            "(follow-for-follow, 'like and repost to win'), or repetitive automated content",
            false="An ordinary post, including someone sharing their own work, an article, or an opinion without pushing a sale or a scheme",
        ),
    ),
    "adult": Noul(
        instructions="Is `post` sexual or sexually explicit? Judge `post.text`, `post.image_alts` and `post.link`.",
        criteria=NoulCriteria(
            true="Sexual content, nudity, pornography, or explicit sexual language, or alt text describing sexual or nude images",
            false="No sexual content; romance, flirting without explicit content, swearing and violence don't count",
        ),
    ),
    "ai_generated": Noul(
        instructions="Was `post.text` written by an AI language model rather than by a person?",
        criteria=NoulCriteria(
            true="The text reads as machine-generated: generic, formulaic, over-polished, or typical chatbot phrasing",
            false="The text reads as written by a person",
        ),
    ),
    # A tone, not a topic: a joke about a trial is still about crime_justice.
    "humor": Noul(
        instructions="Is `post.text` meant as a joke, sarcasm or snark rather than sincerely? Read it in the context of `reply_parent` and `thread_root` when present.",
        criteria=NoulCriteria(
            true="Humor, irony, sarcasm, snark, a quip or a punchline, including deadpan replies in a thread",
            false="A sincere statement, question, announcement or opinion, even a heated or emotional one",
        ),
    ),
    "topic": Choice(instructions=TOPIC_INSTRUCTIONS, criteria=TOPICS),
    "topic_fits": Noul(
        instructions="Is `post` mainly about one of these topics: " + "; ".join(f"{k} ({v})" for k, v in TOPICS.items()) + "?",
        criteria=NoulCriteria(
            true="One of the listed topics clearly describes what the post is about",
            false="None fits: the post is too vague, a bare reaction, a greeting, or about something else",
        ),
    ),
}

# Eval only: the same Choice with the options reversed, to check that the answer
# doesn't depend on option order (Jev leans toward the first option).
ORDER_CHECK: dict[str, Any] = {
    "topic_reversed": Choice(instructions=TOPIC_INSTRUCTIONS, criteria=dict(reversed(list(TOPICS.items())))),
}
