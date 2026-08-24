import json
import pathlib
import subprocess


ROOT = pathlib.Path(__file__).parents[1]
N8N_DIR = ROOT / "scripts" / "n8n"
PATCH_SCRIPT = ROOT / "scripts" / "upsert_deepseek_p1_workflows.ps1"


def _run_js(filename, input_json, build_json=None):
    source = (N8N_DIR / filename).read_text(encoding="utf-8")
    harness = f"""
const input = {json.dumps(input_json)};
const build = {json.dumps(build_json or {})};
const fn = new Function('$input', '$', {json.dumps(source)});
(async () => {{
  try {{
    const value = await fn({{ first: () => ({{ json: input }}) }},
      (name) => ({{ first: () => ({{ json: build }}) }}));
    console.log(JSON.stringify({{ok:true,value}}));
  }} catch (e) {{
    console.log(JSON.stringify({{ok:false,error:e.message}}));
  }}
}})();
"""
    proc = subprocess.run(["node", "-e", harness], capture_output=True, text=True, check=True)
    return json.loads(proc.stdout.strip())


def _run_qa(article_key, site, parsed_json):
    source = (N8N_DIR / "seo_deterministic_qa.js").read_text(encoding="utf-8")
    source = source.replace("__ARTICLE_KEY__", article_key).replace("__SITE__", site)
    harness = f"""
const parsed = {json.dumps(parsed_json)};
const fn = new Function('$', {json.dumps(source)});
const value = fn((name) => ({{ first: () => ({{ json: parsed }}) }}));
console.log(JSON.stringify(value));
"""
    proc = subprocess.run(["node", "-e", harness], capture_output=True, text=True, check=True)
    return json.loads(proc.stdout.strip())[0]["json"]


def test_balance_guard_blocks_unavailable_before_paid_call():
    result = _run_js(
        "seo_balance_guard.js",
        {"is_available": False, "balance_infos": [{"currency": "CNY", "total_balance": "0.00"}]},
        {"requestBody": {"model": "deepseek-chat"}},
    )
    assert result["ok"] is False
    assert "SEO_COST_GATE" in result["error"]


def test_balance_guard_restores_generation_request_when_funded():
    build = {"requestBody": {"model": "deepseek-chat", "max_tokens": 8000}}
    result = _run_js(
        "seo_balance_guard.js",
        {"is_available": True, "balance_infos": [{"currency": "CNY", "total_balance": "25.50"}]},
        build,
    )
    assert result["ok"] is True
    assert result["value"][0]["json"]["requestBody"]["model"] == "deepseek-chat"


def test_balance_guard_fails_closed_on_malformed_or_ambiguous_currency_rows():
    malformed = _run_js(
        "seo_balance_guard.js",
        {"is_available": True, "balance_infos": [{"currency": "CNY", "total_balance": "bad"}]},
        {"requestBody": {"model": "deepseek-chat"}},
    )
    mixed_without_cny = _run_js(
        "seo_balance_guard.js",
        {"is_available": True, "balance_infos": [{"currency": "USD", "total_balance": "25.50"}]},
        {"requestBody": {"model": "deepseek-chat"}},
    )
    assert malformed["ok"] is False
    assert mixed_without_cny["ok"] is False
    assert "SEO_COST_GATE" in malformed["error"]


def test_response_guard_rejects_real_insufficient_balance_shape():
    result = _run_js("seo_response_guard.js", {"error": {"message": "Insufficient Balance"}})
    assert result["ok"] is False
    assert "SEO_RESPONSE_GATE" in result["error"]


def test_response_guard_accepts_complete_two_article_contract():
    text = "===ARTICLE_1===\nTITLE: One\nHTML:\n<p>x</p>\n===ARTICLE_2===\nTITLE: Two\nHTML:\n<p>y</p>"
    result = _run_js(
        "seo_response_guard.js",
        {"choices": [{"message": {"content": text}}]},
    )
    assert result["ok"] is True
    assert result["value"][0]["json"]["rawText"] == text


def test_response_guard_rejects_wrong_order_or_empty_article_body():
    wrong_order = "===ARTICLE_2===\nTITLE: Two\n===ARTICLE_1===\nTITLE: One"
    empty_second = "===ARTICLE_1===\nTITLE: One\n===ARTICLE_2===\n   "
    assert _run_js("seo_response_guard.js", {"choices": [{"message": {"content": wrong_order}}]})["ok"] is False
    assert _run_js("seo_response_guard.js", {"choices": [{"message": {"content": empty_second}}]})["ok"] is False


def test_deterministic_qa_rejects_empty_article_without_model_call():
    source = (N8N_DIR / "seo_deterministic_qa.js").read_text(encoding="utf-8")
    assert "httpRequest" not in source
    assert "api.deepseek.com" not in source
    assert "__ARTICLE_KEY__" in source
    assert "__SITE__" in source

    result = _run_qa("article1", "powkong", {"article1": {}})
    assert result["verdict"] == "reject"
    assert result["quality_score"] < 70


def test_deterministic_qa_passes_a_complete_article_contract():
    html = (
        "<h1>Specific 2026 News</h1>"
        + "<h2>Facts</h2><p>In 2026, price was $49 and latency 10ms.</p>"
        + "<h2>Analysis</h2><p>Useful detail.</p>"
        + "<h2>Impact</h2><p>Useful detail.</p>"
        + "<h2>Recommendation</h2><p>Useful detail.</p>"
        + '<h2 id="sources">Sources</h2>'
        + '<a href="https://a.example">A</a>'
        + '<a href="https://b.example">B</a>'
        + '<a href="https://c.example">C</a>'
        + ("<p>Concrete reporting and useful interpretation.</p>" * 100)
    )
    result = _run_qa(
        "article2",
        "funlab",
        {"article2": {"title": "Title", "slug": "news-title", "meta": "Meta", "excerpt": "Excerpt", "html": html}},
    )
    assert result["verdict"] == "pass"
    assert result["quality_score"] >= 70


def test_deterministic_qa_rejects_missing_excerpt_as_key_field():
    html = (
        "<h1>Specific 2026 News</h1><h2>A</h2><h2>B</h2><h2>C</h2><h2>D</h2>"
        '<h2 id="sources">Sources</h2>'
        '<a href="https://a.example">A</a><a href="https://b.example">B</a><a href="https://c.example">C</a>'
        + ("<p>2026 price $49 latency 10ms.</p>" * 150)
    )
    result = _run_qa(
        "article1",
        "powkong",
        {"article1": {"title": "Title", "slug": "news-title", "meta": "Meta", "excerpt": "", "html": html}},
    )
    assert result["verdict"] == "reject"


def test_deterministic_qa_does_not_count_own_or_duplicate_links_as_sources():
    html = (
        "<h1>Specific 2026 News</h1><h2>A</h2><h2>B</h2><h2>C</h2><h2>D</h2>"
        '<h2 id="sources">Sources</h2>'
        '<a href="https://powkong.com/products/one">Own 1</a>'
        '<a href="https://funlabswitch.com/products/two">Own 2</a>'
        '<a href="https://news.example/story">News</a>'
        '<a href="https://news.example/duplicate">Same domain</a>'
        + ("<p>2026 price $49 latency 10ms.</p>" * 150)
    )
    result = _run_qa(
        "article1",
        "powkong",
        {"article1": {"title": "Title", "slug": "news-title", "meta": "Meta", "excerpt": "Excerpt", "html": html}},
    )
    assert result["verdict"] == "reject"
    assert any("source domains" in issue for issue in result["qa_issues"])


def test_patch_preserves_remote_workflow_and_uses_async_status_polling():
    script = PATCH_SCRIPT.read_text(encoding="utf-8")
    assert "Get-Workflow" in script
    assert "Update-WorkflowSafely" in script
    assert "DeepSeek Balance Preflight" in script
    assert "Require DeepSeek Balance" in script
    assert "Validate DeepSeek Response" in script
    assert "/draft-cleanup/jobs/" in script
    assert "Wait Cleanup" in script
    assert "Get Cleanup Status" in script
    assert "Check Cleanup Deadline" in script
    assert "DRAFT_CLEANUP_TIMEOUT" in script
    assert "activate" in script.lower()
    assert "deactivate" in script.lower()
    assert "'Extract Response'" in script
    assert "'Validate DeepSeek Response'" in script
