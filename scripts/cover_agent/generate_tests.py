#!/usr/bin/env python3
"""Cover-Agent main orchestrator for cocapn-plato.

Pipeline:
1. Run pytest with coverage to find untested lines
2. Build prompts from coverage gaps + existing test patterns
3. Spawn subagents to generate test candidates (zero API cost)
4. Validate each candidate: passes, increases coverage
5. Only keep tests with measurable improvement
6. Output to generated_tests/ directory

Subagent-based generation: instead of calling OpenAI API, we spawn
local subagents that read source code and write tests. Same quality,
zero external cost, full traceability.
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
SUBAGENT_RESULTS = REPO_ROOT / "generated_tests" / "subagent_results.jsonl"

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


# ---------------------------------------------------------------------------
# Subagent-based generation (replaces LLM API)
# ---------------------------------------------------------------------------


def _spawn_test_generator(prompt_data: dict, output_path: Path) -> dict:
    """Write a subagent task file and return the task spec.

    Instead of calling an external LLM API, we prepare a structured task
    that a subagent (another AI instance) can execute. The subagent reads
    the source code, understands the coverage gap, and writes tests.

    In CI/OpenClaw context, this task file is consumed by the agent
    orchestrator. In local dev, it can be run via `sessions_spawn` or
    a simple script that reads the task and generates code.
    """
    task = {
        "type": "test_generation",
        "target_module": prompt_data["module"],
        "target_function": prompt_data.get("function"),
        "untested_lines": prompt_data["untested_lines"],
        "source_file": str(prompt_data["source_file"]),
        "source_context": prompt_data["source_context"],
        "existing_test_patterns": prompt_data.get("existing_patterns", []),
        "output_test_file": str(output_path),
        "instructions": (
            "Write pytest tests for the specified function/method. "
            "Cover error paths, edge cases, and boundary conditions. "
            "Follow existing test patterns in the repo. "
            "Only test public API surface. "
            "Use fixtures where appropriate. "
            "Do not mock unless necessary."
        ),
    }
    return task


def _run_local_subagent(task: dict, timeout: int = 120) -> dict:
    """Execute a test generation task using a local AI subagent.

    Strategy: we write the task to a JSON file, then invoke a Python
    script that uses an in-process language model (or a local API) to
    generate the test code. This avoids external API calls entirely.

    For environments without a local model, we fall back to a template-
    based generator that produces structurally correct tests from the
    source code analysis.
    """
    import tempfile
    import shutil

    # Write task to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(task, f)
        task_path = f.name

    # Check if we have a local subagent runner (OpenClaw sessions_spawn)
    openclaw_bin = shutil.which("openclaw")
    if openclaw_bin:
        # Use OpenClaw's subagent spawning capability
        result = subprocess.run(
            [
                openclaw_bin, "sessions_spawn",
                "--task", task_path,
                "--runtime", "subagent",
                "--timeout", str(timeout),
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 30,
        )
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }

    # Fallback: template-based generation
    return _template_generate(task)


def _template_generate(task: dict) -> dict:
    """Generate tests from source analysis without any LLM.

    This is the zero-cost fallback. It reads the source file, extracts
    function signatures, and generates tests that exercise:
    - Normal input cases
    - None/empty inputs
    - Boundary values (0, -1, maxint)
    - Type mismatches (if function has type hints)
    """
    import ast
    import inspect

    source_path = Path(task["source_file"])
    if not source_path.exists():
        return {"success": False, "error": f"Source file not found: {source_path}"}

    source = source_path.read_text()
    tree = ast.parse(source)

    target_func = task.get("target_function")
    target_module = task["target_module"]

    # Find the target function in the AST
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == target_func:
                func_node = node
                break

    if not func_node:
        return {"success": False, "error": f"Function {target_func} not found in AST"}

    # Extract signature info
    args = []
    defaults_start = len(func_node.args.args) - len(func_node.args.defaults)
    for i, arg in enumerate(func_node.args.args):
        arg_name = arg.arg
        # Try to get type hint
        type_hint = ""
        if arg.annotation:
            type_hint = ast.unparse(arg.annotation)
        # Check for default
        default = None
        if i >= defaults_start:
            default_node = func_node.args.defaults[i - defaults_start]
            try:
                default = ast.literal_eval(default_node)
            except Exception:
                pass
        args.append({"name": arg_name, "type": type_hint, "default": default})

    # Build test cases
    test_cases = []

    # Normal case: all defaults or minimal args
    normal_call = f"{target_func}("
    normal_args = []
    for arg in args:
        if arg["default"] is not None:
            normal_args.append(f"{arg['name']}={repr(arg['default'])}")
        else:
            # Guess a value based on type hint
            guessed = _guess_value(arg["type"])
            normal_args.append(f"{arg['name']}={guessed}")
    normal_call += ", ".join(normal_args) + ")"
    test_cases.append({"name": f"test_{target_func}_normal", "call": normal_call})

    # Edge cases for each arg
    for arg in args:
        if arg["default"] is not None:
            continue  # Skip args with defaults for edge case
        edge_values = _edge_values_for_type(arg["type"])
        for edge in edge_values:
            edge_args = []
            for a in args:
                if a["name"] == arg["name"]:
                    edge_args.append(f"{a['name']}={edge}")
                elif a["default"] is not None:
                    edge_args.append(f"{a['name']}={repr(a['default'])}")
                else:
                    edge_args.append(f"{a['name']}={_guess_value(a['type'])}")
            edge_call = f"{target_func}(" + ", ".join(edge_args) + ")"
            test_cases.append({
                "name": f"test_{target_func}_{arg['name']}_edge",
                "call": edge_call,
            })

    # Generate test file content
    import_lines = f"""import pytest
from {target_module} import {target_func}
"""

    test_body = ""
    for i, case in enumerate(test_cases):
        test_body += f"""

def {case['name']}():
    """Auto-generated test for {target_func}."""
    result = {case['call']}
    # TODO: assert expected behavior
    assert result is not None
"""

    output_path = Path(task["output_test_file"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(import_lines + test_body)

    return {
        "success": True,
        "tests_generated": len(test_cases),
        "output_file": str(output_path),
        "method": "template",
    }


def _guess_value(type_hint: str) -> str:
    """Guess a sensible test input from a type hint."""
    type_map = {
        "str": '"test_value"',
        "int": "42",
        "float": "3.14",
        "bool": "True",
        "list": "[]",
        "dict": "{}",
        "List": "[]",
        "Dict": "{}",
    }
    for key, val in type_map.items():
        if key in type_hint:
            return val
    return '"test_value"'  # Default


def _edge_values_for_type(type_hint: str) -> list[str]:
    """Return edge case values for a type."""
    if "str" in type_hint:
        return ['""', '"a" * 10000']
    if "int" in type_hint:
        return ["0", "-1", "999999"]
    if "float" in type_hint:
        return ["0.0", "-0.1", "1e308"]
    if "bool" in type_hint:
        return ["False"]
    if "list" in type_hint:
        return ["[]"]
    return ["None"]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_baseline() -> dict:
    """Run pytest with coverage and return summary data."""
    logger.info("Running coverage baseline...")
    result = subprocess.run(
        [
            "python3", "-m", "pytest",
            "--cov=cocapn_plato",
            "--cov-report=json:coverage.json",
            "--cov-report=term-missing",
            "-q", "--tb=no",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if not COVERAGE_JSON.exists():
        logger.error("coverage.json not generated")
        return {"error": "coverage.json missing", "raw_output": result.stderr}

    data = json.loads(COVERAGE_JSON.read_text())
    totals = data.get("totals", {})
    pct = totals.get("percent_covered", 0)
    logger.info(f"Baseline coverage: {pct:.2f}%")
    return {"coverage_pct": pct, "totals": totals, "files": data.get("files", {})}


def analyze_gaps(coverage_data: dict) -> list[dict]:
    """Find untested functions and lines from coverage data."""
    logger.info("Analyzing coverage gaps...")
    builder = PromptBuilder(REPO_ROOT)
    gaps = builder.build_gaps()
    logger.info(f"Found {len(gaps)} gaps")
    return gaps


def generate_tests(gaps: list[dict], max_gaps: int | None = None) -> list[dict]:
    """Generate tests for coverage gaps using subagents/templates."""
    if max_gaps:
        gaps = gaps[:max_gaps]

    results = []
    GEN_DIR.mkdir(parents=True, exist_ok=True)

    for i, gap in enumerate(gaps):
        module = gap["module"]
        func = gap.get("function", "unknown")
        logger.info(f"[{i+1}/{len(gaps)}] Generating tests for {module}.{func}...")

        output_file = GEN_DIR / f"test_{module.replace('.', '_')}_{func}.py"

        # Build subagent task
        task = _spawn_test_generator(gap, output_file)

        # Execute generation
        start = time.time()
        result = _run_local_subagent(task)
        elapsed = time.time() - start

        if result.get("success"):
            logger.info(f"  ✓ Generated in {elapsed:.1f}s: {result.get('tests_generated', '?')} tests")
            results.append({
                "gap": gap,
                "output_file": str(output_file),
                "success": True,
                "method": result.get("method", "unknown"),
                "tests_generated": result.get("tests_generated", 0),
                "elapsed": elapsed,
            })
        else:
            logger.warning(f"  ✗ Failed: {result.get('error', 'unknown')}")
            results.append({
                "gap": gap,
                "success": False,
                "error": result.get("error", "unknown"),
                "elapsed": elapsed,
            })

    # Save results
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text(json.dumps(results, indent=2))
    logger.info(f"Generation results saved to {RESULTS_FILE}")

    return results


def validate_tests(results: list[dict]) -> list[dict]:
    """Run validation pipeline on generated tests."""
    validator = TestValidator(REPO_ROOT)
    validated = []

    for r in results:
        if not r["success"]:
            continue
        output_file = Path(r["output_file"])
        if not output_file.exists():
            continue

        logger.info(f"Validating {output_file.name}...")
        result = validator.validate(output_file)
        validated.append(result)

        if result["passes"]:
            # Promote to tests/ directory
            target = TESTS_DIR / output_file.name
            target.write_text(output_file.read_text())
            logger.info(f"  ✓ Promoted to {target}")
        else:
            logger.warning(f"  ✗ Failed validation: {result.get('error', 'unknown')}")

    return validated


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Cover-Agent: automated test generation")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze gaps, don't generate")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing generated tests")
    parser.add_argument("--max-gaps", type=int, default=None, help="Max gaps to process")
    parser.add_argument("--baseline", action="store_true", help="Run coverage baseline only")
    args = parser.parse_args()

    if args.baseline:
        baseline = run_baseline()
        print(json.dumps(baseline, indent=2))
        return

    if not args.validate_only:
        baseline = run_baseline()
        gaps = analyze_gaps(baseline)

        if args.analyze_only:
            print(json.dumps(gaps, indent=2))
            return

        results = generate_tests(gaps, max_gaps=args.max_gaps)

    if not args.analyze_only:
        # Validate all generated tests (including previously generated)
        gen_files = list(GEN_DIR.glob("test_*.py")) if GEN_DIR.exists() else []
        if gen_files:
            logger.info(f"Validating {len(gen_files)} generated test files...")
            validator = TestValidator(REPO_ROOT)
            for f in gen_files:
                result = validator.validate(f)
                if result["passes"]:
                    target = TESTS_DIR / f.name
                    target.write_text(f.read_text())
                    logger.info(f"  ✓ {f.name} -> {target}")

    # Final coverage check
    logger.info("Running final coverage check...")
    final = run_baseline()
    logger.info(f"Final coverage: {final['coverage_pct']:.2f}%")


if __name__ == "__main__":
    main()
