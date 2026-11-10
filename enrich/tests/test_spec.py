import duckdb
import pytest

from bsky_enrich import evaluate, spec


@pytest.mark.parametrize(
    ("post", "expected"),
    [
        ({"text": "lol"}, False),
        ({"text": "x" * spec.MIN_CONTENT_CHARS}, True),
        ({"text": "lol", "external_uri": "https://example.com/a"}, True),  # a link card is content
        ({"text": "look", "image_alts": ["a long description of a sunset over the bay"]}, True),
        ({"text": None, "image_alts": [None, ""]}, False),
    ],
)
def test_eligible(post, expected):
    assert spec.eligible(post) is expected


def test_eligible_sql_agrees_with_python():
    posts = [
        ("lol", None, [], None),
        ("x" * spec.MIN_CONTENT_CHARS, None, [], None),
        ("lol", "https://example.com/a", [], None),
        ("look", None, ["a long description of a sunset over the bay"], None),
        ("hi", None, [], "A title that is long enough by itself"),
    ]
    con = duckdb.connect()
    con.execute("CREATE TABLE p (text VARCHAR, external_uri VARCHAR, image_alts VARCHAR[], external_title VARCHAR)")
    con.executemany("INSERT INTO p VALUES (?, ?, ?, ?)", posts)
    got = [r[0] for r in con.sql(f"SELECT {spec.ELIGIBLE_SQL} FROM p").fetchall()]
    want = [spec.eligible(dict(zip(["text", "external_uri", "image_alts", "external_title"], p))) for p in posts]
    assert got == want == [False, True, True, True, True]


def test_state_names_what_the_questions_reference_and_leaves_out_empty_fields():
    post = {
        "text": "New post", "langs": ["en"], "image_alts": ["a cat", ""], "external_uri": "https://www.example.com/x",
        "external_title": "Example", "self_labels": ["porn"],
    }
    s = spec.state(post, display_name="Ann", quoted_text="quoted", parent_text=None)
    assert s == {
        "post": {"text": "New post", "langs": ["en"], "image_alts": ["a cat"], "link": {"title": "Example", "domain": "example.com"},
                 "quoted_post_text": "quoted"},
        "author": {"display_name": "Ann"},
    }
    assert spec.state({"text": "hi"}) == {"post": {"text": "hi"}}
    # The thread root only when it isn't the parent itself.
    assert spec.state({"text": "hi"}, parent_text="p", root_text="r")["thread_root"] == {"text": "r"}
    assert "thread_root" not in spec.state({"text": "hi"}, parent_text="p", root_text="p")


def test_every_question_references_the_state_by_path():
    for qid, q in (spec.QUESTIONS | spec.ORDER_CHECK).items():
        assert "`post" in q.instructions, qid
    assert list(spec.ORDER_CHECK["topic_reversed"].criteria) == list(reversed(spec.TOPICS))


def test_precision_recall():
    pairs = [(0.9, True), (0.8, False), (0.4, True), (0.1, False)]
    assert evaluate.precision_recall(pairs, 0.7) == (0.5, 0.5, 2)
    assert evaluate.precision_recall(pairs, 0.95) == (None, 0.0, 0)


def test_report_runs_on_a_small_set():
    def answer(spam, adult, ai, topic, fits):
        choice = {"choice": topic, "probabilities": {topic: 0.9}, "confidence": 0.9}
        return {"spam": spam, "adult": adult, "ai_generated": ai, "humor": 0.5, "topic": choice, "topic_reversed": choice,
                "topic_fits": fits, "input_tokens": 600}

    labels = [
        {"uri": "a", "spam": True, "adult": False, "ai_generated": None, "humor": False, "topic": "tech", "topic_alt": "business"},
        {"uri": "b", "spam": False, "adult": True, "ai_generated": False, "humor": True, "topic": None},
    ]
    answers = {"a": answer(0.95, 0.02, 0.5, "business", 0.9), "b": answer(0.1, 0.9, 0.2, "personal", 0.1)}
    records = {"a": {"stratum": "random", "self_labels": []}, "b": {"stratum": "adult_signal", "self_labels": ["porn"]}}
    text = evaluate.report(labels, answers, records)
    assert "spam: 1 of 2 labeled yes" in text
    assert "topic: 1 of 1 correct" in text and "0 matching the first label" in text  # the alternative counts
    assert "self-labeled" in text and "per million posts" in text
