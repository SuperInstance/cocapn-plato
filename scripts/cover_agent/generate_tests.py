"""Cover-Agent main orchestrator for cocapn-plato.

Pipeline:
1. Run pytest with coverage to find untested lines
2. Build prompts from coverage gaps + existing test patterns
3. Call LLM API (via LiteLLM) to generate test candidates
4. Validate each candidate: passes, increases coverage
5. Only keep tests with measurable improvement
6. Output to generated_tests/ directory

Cost budget: max $5 per run (~1M tokens at gpt-4o-mini rates).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TESTS_DIR = REPO_ROOT / "tests"
GEN_DIR = REPO_ROOT / "generated_tests"
COVERAGE_JSON = REPO_ROOT / "coverage.json"
COVERAGE_GAPS_JSON = REPO_ROOT / "coverage_report" / "coverage_gaps.json"
PROMPTS_FILE = REPO_ROOT / "generated_tests" / "prompts.jsonl"

sys.path.insert(0, str(REPO_ROOT / "scripts" / "cover_agent"))

from prompt_builder import PromptBuilder  # noqa: E402
from validate_tests import TestValidator  # noqa: E402

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cover_agent")

RESULTS_FILE = REPO_ROOT / "generated_tests" / "validation_results.json"
logger = logging.getLogger("cover_agent")


# ---------------------------------------------------------------------------
# LiteLLM / LLM integration
# ---------------------------------------------------------------------------


def _get_llm_client() -> Any:
    """Return a LiteLLM-compatible completion function.

    We try litellm first, then fall back to a raw requests-based client
    if the package isn't installed.
    """
    try:
        import litellm

        # LiteLLM can be chatty; tone it down
        litellm.suppress_debug_info = True
        litellm.set_verbose = False
        return litellm.completion
    except ImportError:
        logger.warning("litellm not installed; falling back to requests-based client")
        return _raw_completion


def _raw_completion(
    model: str,
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 2048,
    **kwargs: Any,
) -> Any:
    """Bare-bones HTTP client for OpenAI-compatible APIs.

    Only used when litellm is not available.
    """
    import requests

    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LITELLM_API_KEY")
    base_url = os.environ.get("LITELLM_API_BASE", "https://api.openai.com/v1")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    response = requests.post(
        f"{base_url}/chat/completions",
        headers=headers,
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def _extract_content(completion_response: Any) -> str:
    """Extract text content from LiteLLM or raw OpenAI response."""
    if isinstance(completion_response, dict):
        choices = completion_response.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            return msg.get("content", "")
        return ""
    # LiteLLM model response object
    try:
        return completion_response.choices[0].message.content or ""
    except Exception:
        return ""


def call_llm(
    prompt: str,
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_tokens: int = 2048,
    max_retries: int = 3,
) -> str:
    """Send a prompt to the LLM and return the generated text."""
    client = _get_llm_client()
    messages = [
        {
            "role": "system",
            "content": (
                "You are a senior Python test engineer. "
                "Write pytest tests that increase code coverage. "
                "Be concise, deterministic, and focus on edge cases."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    for attempt in range(max_retries):
        try:
            logger.info(f"LLM call: model={model} attempt={attempt + 1}/{max_retries}")
            start = time.time()
            resp = client(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            elapsed = time.time() - start
            content = _extract_content(resp)
            tokens_used = _estimate_tokens(prompt) + _estimate_tokens(content)
            cost = _estimate_cost(model, tokens_used)
            logger.info(f"LLM responded in {elapsed:.1f}s ~{tokens_used} tokens ~${cost:.4f}")
            return content
        except Exception as exc:
            logger.warning(f"LLM call failed (attempt {attempt + 1}): {exc}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise

    return ""


def _estimate_tokens(text: str) -> int:
    """Rough token estimate (GPT-style ~4 chars/token)."""
    return len(text) // 4


def _estimate_cost(model: str, tokens: int) -> float:
    """Rough cost estimate in USD."""
    # gpt-4o-mini: ~$0.15 / 1M input tokens, ~$0.60 / 1M output
    # claude-3-haiku: ~$0.25 / 1M input, ~$1.25 / 1M output
    # We average input + output; this is a rough upper bound
    rates = {
        "gpt-4o-mini": 0.60 / 1_000_000,
        "gpt-4o": 5.00 / 1_000_000,
        "claude-3-haiku": 1.25 / 1_000_000,
        "claude-3-sonnet": 3.00 / 1_000_000,
    }
    rate = rates.get(model, 1.00 / 1_000_000)
    return tokens * rate


# ---------------------------------------------------------------------------
# Coverage baseline
# ---------------------------------------------------------------------------


def run_coverage_baseline() -> float:
    """Run the full test suite and produce coverage.json.

    Returns the total coverage percentage.
    """
    logger.info("Running coverage baseline...")
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        str(TESTS_DIR),
        "-q",
        f"--cov={REPO_ROOT / 'src' / 'cocapn_plato'}",
        "--cov-report=json",
        "--cov-report=term",
    ]
    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
    )
    if result.returncode not in (0, 1):  # pytest exits 1 on coverage failure
        logger.warning(f"Baseline run had issues: {result.stderr[:500]}")

    pct = _read_total_coverage()
    logger.info(f"Baseline coverage: {pct:.2f}%")
    return pct


def _read_total_coverage() -> float:
    """Read total coverage from coverage.json."""
    if not COVERAGE_JSON.exists():
        return 0.0
    data = json.loads(COVERAGE_JSON.read_text(encoding="utf-8"))
    return float(data.get("totals", {}).get("percent_covered", 0.0))


# ---------------------------------------------------------------------------
# Gap analysis
# ---------------------------------------------------------------------------


def _find_gaps(threshold: float = 75.0) -> list[dict[str, Any]]:
    """Parse coverage.json and return sorted list of gap dicts."""
    if not COVERAGE_JSON.exists():
        logger.error("coverage.json not found. Run `make coverage` first.")
        sys.exit(1)

    data = json.loads(COVERAGE_JSON.read_text(encoding="utf-8"))
    gaps: list[dict[str, Any]] = []

    for file_key, file_data in data.get("files", {}).items():
        summary = file_data.get("summary", {})
        pct = summary.get("percent_covered", 100.0)
        if pct >= threshold:
            continue

        missing_lines = file_data.get("missing_lines", [])
        if not missing_lines:
            continue

        rel = file_key.replace("src/", "").replace("/", ".").replace(".py", "")
        gaps.append({
            "file_path": file_key,
            "module_name": rel,
            "current_coverage_pct": pct,
            "missing_lines": missing_lines,
        })

    gaps.sort(key=lambda g: g["current_coverage_pct"])
    return gaps


def save_gaps_json(gaps: list[dict[str, Any]]) -> None:
    """Write structured gap data to coverage_report/coverage_gaps.json."""
    COVERAGE_GAPS_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "threshold": 75.0,
        "total_gaps": len(gaps),
        "zero_coverage_modules": [g["module_name"] for g in gaps if g["current_coverage_pct"] == 0.0],
        "gaps_below_50": [g["module_name"] for g in gaps if g["current_coverage_pct"] < 50.0],
        "gaps": gaps,
    }
    COVERAGE_GAPS_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(f"Saved gap analysis to {COVERAGE_GAPS_JSON}")


# ---------------------------------------------------------------------------
# Generation loop
# ---------------------------------------------------------------------------


def generate_tests(
    gaps: list[dict[str, Any]],
    model: str = "gpt-4o-mini",
    max_gaps: int = 10,
    budget_usd: float = 5.0,
) -> list[tuple[str, str]]:
    """Generate test candidates for the biggest coverage gaps.

    Returns list of (module_name, llm_response) tuples.
    """
    builder = PromptBuilder(repo_root=REPO_ROOT)

    # Sort by lowest coverage, take top N
    targets = gaps[:max_gaps]
    logger.info(f"Targeting top {len(targets)} gaps for generation")

    responses: list[tuple[str, str]] = []
    total_cost = 0.0

    for gap in targets:
        source_path = REPO_ROOT / gap["file_path"]
        prompt = builder.build(
            module=gap["module_name"],
            source_path=source_path,
            missing_lines=gap["missing_lines"],
            existing_tests_dir=TESTS_DIR,
        )

        estimated_cost = _estimate_cost(model, _estimate_tokens(prompt) + 2048)
        if total_cost + estimated_cost > budget_usd:
            logger.warning(f"Budget cap reached (${total_cost:.2f} / ${budget_usd}). Stopping.")
            break

        response = call_llm(prompt, model=model)
        responses.append((gap["module_name"], response))
        total_cost += estimated_cost
        time.sleep(1)  # Rate-limit politeness

    logger.info(f"Generation complete. Estimated spend: ${total_cost:.2f}")
    return responses


# ---------------------------------------------------------------------------
# Validation + promotion
# ---------------------------------------------------------------------------


def _write_generated_test(module_name: str, code: str) -> Path:
    """Write a generated test file to generated_tests/."""
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    short = module_name.split(".")[-1]
    fname = f"test_{short}_generated.py"

    # If file already exists, add a counter
    out_path = GEN_DIR / fname
    counter = 1
    while out_path.exists():
        out_path = GEN_DIR / f"test_{short}_generated_{counter}.py"
        counter += 1

    out_path.write_text(code, encoding="utf-8")
    return out_path


def _extract_code_blocks(text: str) -> list[str]:
    """Extract python code blocks from LLM markdown response."""
    import re

    # Try to find code blocks
    pattern = re.compile(r"```(?:python)?\n(.*?)\n```", re.DOTALL)
    matches = pattern.findall(text)
    if matches:
        return [m.strip() for m in matches]

    # If no markdown fences, assume the whole thing is code
    stripped = text.strip()
    if stripped.startswith("def test_") or stripped.startswith("import "):
        return [stripped]

    return []


def validate_and_promote(
    responses: list[tuple[str, str]],
    baseline_coverage: float,
    promote: bool = True,
) -> list[dict[str, Any]]:
    """Validate all generated candidates and promote passing ones to tests/."""
    validator = TestValidator(repo_root=REPO_ROOT)
    all_results: list[dict[str, Any]] = []

    for module_name, response in responses:
        code_blocks = _extract_code_blocks(response)
        if not code_blocks:
            logger.warning(f"No code blocks extracted for {module_name}")
            all_results.append({
                "passed": False,
                "coverage_delta": 0.0,
                "error": "No code blocks in LLM response",
                "module": module_name,
                "file": None,
            })
            continue

        for code in code_blocks:
            test_path = _write_generated_test(module_name, code)
            result = validator.validate(test_path)
            result["module"] = module_name
            result["file"] = str(test_path.name)
            all_results.append(result)

            if result["passed"] and promote:
                dst = TESTS_DIR / test_path.name
                if dst.exists():
                    logger.warning(f"Destination exists, skipping promotion: {dst}")
                    continue
                code = test_path.read_text(encoding="utf-8")
                dst.write_text(code, encoding="utf-8")
                logger.info(f"Promoted {test_path.name} → tests/ (+{result['coverage_delta']:.2f}%)")

    return all_results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Cover-Agent: auto-generate tests for untested code paths.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --analyze-only                # Just analyze gaps, don't generate
  %(prog)s --max-gaps 5 --model gpt-4o-mini   # Generate for top 5 gaps
  %(prog)s --budget 2.50                 # Tighter budget
        """,
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="Only run coverage and save gap analysis, skip generation",
    )
    parser.add_argument(
        "--baseline-only",
        action="store_true",
        help="Only compute and print baseline coverage",
    )
    parser.add_argument(
        "--max-gaps",
        type=int,
        default=10,
        help="Maximum gaps to target (default: 10)",
    )
    parser.add_argument(
        "--model",
        default="gpt-4o-mini",
        help="LLM model via LiteLLM (default: gpt-4o-mini)",
    )
    parser.add_argument(
        "--budget",
        type=float,
        default=5.0,
        help="Max USD spend per run (default: 5.0)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=75.0,
        help="Coverage threshold to consider a gap (default: 75.0)",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing generated_tests/, skip generation",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        default=True,
        help="Copy passing tests into tests/ (default: True)",
    )
    parser.add_argument(
        "--no-promote",
        action="store_false",
        dest="promote",
        help="Keep generated tests in generated_tests/ only",
    )
    args = parser.parse_args()

    # Verify LiteLLM / API key
    if not args.analyze_only and not args.baseline_only and not args.validate_only:
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LITELLM_API_KEY")
        if not api_key:
            logger.error(
                "No API key found. Set OPENAI_API_KEY or LITELLM_API_KEY.\n"
                "If you want to run without an LLM, use --analyze-only."
            )
            sys.exit(1)

    # Phase 1: baseline
    baseline = run_coverage_baseline()

    if args.baseline_only:
        print(f"Baseline coverage: {baseline:.2f}%")
        sys.exit(0)

    # Phase 2: gap analysis
    gaps = _find_gaps(args.threshold)
    save_gaps_json(gaps)
    logger.info(f"Found {len(gaps)} coverage gaps below {args.threshold}%")

    if args.analyze_only:
        print(f"Coverage: {baseline:.2f}%")
        print(f"Gaps below {args.threshold}%: {len(gaps)}")
        zero = [g for g in gaps if g["current_coverage_pct"] == 0.0]
        print(f"  Zero-coverage modules: {len(zero)}")
        for g in zero[:10]:
            print(f"    - {g['module_name']} ({g['file_path']})")
        sys.exit(0)

    if args.validate_only:
        validator = TestValidator(repo_root=REPO_ROOT)
        results = validator.validate_batch(GEN_DIR)
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            print(f"[{status}] {r['file']}  delta={r['coverage_delta']:+.2f}%")
        sys.exit(0)

    # Phase 3: generate
    responses = generate_tests(
        gaps,
        model=args.model,
        max_gaps=args.max_gaps,
        budget_usd=args.budget,
    )

    if not responses:
        logger.info("No LLM responses — nothing to validate.")
        sys.exit(0)

    # Phase 4: validate + promote
    all_results = validate_and_promote(responses, baseline, promote=args.promote)

    # Phase 5: report
    passed = [r for r in all_results if r["passed"]]
    failed = [r for r in all_results if not r["passed"]]
    total_delta = sum(r["coverage_delta"] for r in passed)

    summary = {
        "baseline_coverage": baseline,
        "gaps_targeted": args.max_gaps,
        "generated": len(all_results),
        "passed": len(passed),
        "failed": len(failed),
        "coverage_delta_total": total_delta,
        "results": all_results,
    }

    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info(f"Saved results to {RESULTS_FILE}")

    print("\n" + "=" * 60)
    print("Cover-Agent Summary")
    print("=" * 60)
    print(f"Baseline coverage:   {baseline:.2f}%")
    print(f"Tests generated:     {len(all_results)}")
    print(f"Tests passed:        {len(passed)}")
    print(f"Coverage delta:      +{total_delta:.2f}%")
    print(f"Results JSON:        {RESULTS_FILE}")
    print("=" * 60)

    # Exit 1 if nothing passed (CI signal)
    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    _main()
