#!/usr/bin/env python3
"""Test validation pipeline for Cover-Agent generated tests.

Runs generated test candidates in isolation, verifies they pass,
and checks whether coverage actually increased.

Also exports functional API used by generate_tests.py.
"""

from __future__ import annotations

import json
import logging
import os
import re as _re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("cover_agent.validate_tests")


# ---------------------------------------------------------------------------
# Dataclass (used by generate_tests.py)
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Result of validating a single generated test candidate."""

    passed: bool
    coverage_delta: float
    error_message: str | None
    test_file: str | None


# ---------------------------------------------------------------------------
# TestValidator class (primary interface)
# ---------------------------------------------------------------------------


class TestValidator:
    """Validates a generated test file for correctness and coverage impact."""

    def __init__(self, repo_root: Path, timeout: int = 60):
        self.repo_root = Path(repo_root)
        self.timeout = timeout
        self._baseline_coverage: float | None = None

    def _get_baseline(self) -> float:
        """Run coverage once and cache the baseline percentage."""
        if self._baseline_coverage is not None:
            return self._baseline_coverage
        self._baseline_coverage = self._run_coverage(test_path=None)
        logger.debug("Baseline coverage: %.2f%%", self._baseline_coverage)
        return self._baseline_coverage

    def _run_coverage(self, test_path: Path | None) -> float:
        """Run pytest with coverage and return total percent_covered."""
        cmd = [
            sys.executable, "-m", "pytest",
            "--cov=cocapn_plato",
            "--cov-report=json",
            "-q",
        ]
        if test_path:
            cmd.append(str(test_path))
        env = dict(os.environ)
        env["PYTHONPATH"] = str(self.repo_root / "src")
        subprocess.run(
            cmd,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            env=env,
            timeout=self.timeout,
        )
        coverage_json = self.repo_root / "coverage.json"
        if not coverage_json.exists():
            return 0.0
        with open(coverage_json) as f:
            data = json.load(f)
        return data.get("totals", {}).get("percent_covered", 0.0)

    def _run_test(self, test_path: Path) -> dict[str, Any]:
        """Run the test file in an isolated pytest process."""
        cmd = [
            sys.executable, "-m", "pytest",
            str(test_path),
            "-v",
            "--tb=short",
        ]
        env = dict(os.environ)
        env["PYTHONPATH"] = str(self.repo_root / "src")
        result = subprocess.run(
            cmd,
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            env=env,
            timeout=self.timeout,
        )
        return {
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "passed": result.returncode == 0,
        }

    def validate(self, test_path: Path) -> dict[str, Any]:
        """Validate a single generated test file."""
        test_path = Path(test_path)
        if not test_path.exists():
            return {"passed": False, "coverage_delta": 0.0, "error": f"File not found: {test_path}", "exit_code": -1}

        # Syntax check
        try:
            with open(test_path) as f:
                source = f.read()
            compile(source, str(test_path), "exec")
        except SyntaxError as exc:
            return {"passed": False, "coverage_delta": 0.0, "error": f"Syntax error: {exc}", "exit_code": -1}

        # Run test in isolation
        run_result = self._run_test(test_path)
        if not run_result["passed"]:
            error = (run_result["stderr"] or run_result["stdout"]).strip()
            if len(error) > 2000:
                error = error[:2000] + "\n...[truncated]"
            return {
                "passed": False,
                "coverage_delta": 0.0,
                "error": error,
                "exit_code": run_result["exit_code"],
            }

        # Coverage delta
        baseline = self._get_baseline()
        with_test = self._run_coverage(test_path)
        delta = with_test - baseline

        if delta <= 0:
            return {
                "passed": False,
                "coverage_delta": delta,
                "error": f"No coverage increase ({with_test:.2f}% vs baseline {baseline:.2f}%)",
                "exit_code": run_result["exit_code"],
            }

        logger.info("Test %s validated: +%.2f%% coverage", test_path.name, delta)
        return {
            "passed": True,
            "coverage_delta": delta,
            "error": None,
            "exit_code": run_result["exit_code"],
        }

    def validate_batch(self, test_dir: Path) -> list[dict[str, Any]]:
        """Validate all generated tests in a directory."""
        results = []
        for test_file in sorted(Path(test_dir).glob("test_*.py")):
            result = self.validate(test_file)
            result["file"] = str(test_file.name)
            results.append(result)
        return results


# ---------------------------------------------------------------------------
# Functional API (used by generate_tests.py)
# ---------------------------------------------------------------------------


def _find_repo_root() -> Path:
    """Walk up from cwd to find the repo root."""
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return cwd


REPO_ROOT = _find_repo_root()
TESTS_DIR = REPO_ROOT / "tests"
GEN_DIR = REPO_ROOT / "generated_tests"
COVERAGE_JSON = REPO_ROOT / "coverage.json"


def extract_code_blocks(text: str) -> list[tuple[str, str]]:
    """Extract markdown code blocks from LLM response."""
    pattern = _re.compile(r"```(?:python)?\n(.*?)\n```", _re.DOTALL)
    matches = pattern.findall(text)
    return [("python", m.strip()) for m in matches]


def parse_file_header(text: str) -> str | None:
    """Look for a header like '### File: tests/test_foo_generated.py'."""
    match = _re.search(r"###\s*File:\s*(\S+)", text)
    return match.group(1) if match else None


def write_isolated_test(code: str, suggested_name: str | None = None) -> Path:
    """Write a generated test to generated_tests/ and return its path."""
    GEN_DIR.mkdir(parents=True, exist_ok=True)
    if suggested_name:
        fname = Path(suggested_name).name
        if not fname.startswith("test_"):
            fname = f"test_{fname}"
        if not fname.endswith(".py"):
            fname += ".py"
    else:
        import hashlib

        h = hashlib.sha256(code.encode()).hexdigest()[:12]
        fname = f"test_generated_{h}.py"
    out_path = GEN_DIR / fname
    out_path.write_text(code, encoding="utf-8")
    return out_path


def run_test_file(test_path: Path, timeout: int = 60) -> tuple[bool, str]:
    """Run a single test file in isolation via pytest."""
    cmd = [
        sys.executable, "-m", "pytest",
        str(test_path), "-v", "--tb=short", "--no-header",
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
        )
        return (result.returncode == 0, result.stdout + result.stderr)
    except subprocess.TimeoutExpired:
        return False, f"Timeout after {timeout}s"
    except Exception as exc:
        return False, str(exc)


def get_coverage_pct(coverage_json_path: Path) -> float:
    """Read total coverage percentage from coverage JSON."""
    if not coverage_json_path.exists():
        return 0.0
    data = json.loads(coverage_json_path.read_text(encoding="utf-8"))
    return float(data.get("totals", {}).get("percent_covered", 0.0))


def run_with_coverage(
    test_path: Path,
    baseline_coverage: float,
    timeout: int = 120,
) -> ValidationResult:
    """Run a test file and verify it increases coverage."""
    coverage_json_tmp = REPO_ROOT / ".coverage_gen.json"
    cmd = [
        sys.executable, "-m", "pytest",
        str(test_path), "-v", "--tb=short", "--no-header",
        f"--cov={REPO_ROOT / 'src' / 'cocapn_plato'}",
        "--cov-report=json",
    ]
    try:
        subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
        )
    except subprocess.TimeoutExpired:
        return ValidationResult(
            passed=False, coverage_delta=0.0,
            error_message=f"Coverage run timed out after {timeout}s",
            test_file=str(test_path),
        )
    except Exception as exc:
        return ValidationResult(
            passed=False, coverage_delta=0.0,
            error_message=str(exc),
            test_file=str(test_path),
        )

    new_coverage = 0.0
    if coverage_json_tmp.exists():
        new_coverage = get_coverage_pct(coverage_json_tmp)
        coverage_json_tmp.unlink(missing_ok=True)

    passes, output = run_test_file(test_path)
    if not passes:
        return ValidationResult(
            passed=False, coverage_delta=0.0,
            error_message=f"Test execution failed:\n{output}",
            test_file=str(test_path),
        )

    delta = new_coverage - baseline_coverage
    if delta <= 0:
        return ValidationResult(
            passed=False, coverage_delta=delta,
            error_message=f"Coverage did not increase ({baseline_coverage:.2f}% → {new_coverage:.2f}%)",
            test_file=str(test_path),
        )

    return ValidationResult(
        passed=True, coverage_delta=delta,
        error_message=None,
        test_file=str(test_path),
    )


def validate_llm_output(
    llm_response: str,
    baseline_coverage: float | None = None,
    auto_run: bool = True,
) -> list[ValidationResult]:
    """Full pipeline: extract code blocks from LLM response → write → validate."""
    if baseline_coverage is None:
        baseline_coverage = _compute_baseline_coverage()

    blocks = extract_code_blocks(llm_response)
    results: list[ValidationResult] = []

    for _, code in blocks:
        header_file = parse_file_header(code)
        if not header_file:
            header_file = parse_file_header(llm_response)
        test_path = write_isolated_test(code, suggested_name=header_file)

        if auto_run:
            result = run_with_coverage(test_path, baseline_coverage)
        else:
            result = ValidationResult(
                passed=False, coverage_delta=0.0,
                error_message="Validation skipped (auto_run=False)",
                test_file=str(test_path),
            )
        results.append(result)

    return results


def _compute_baseline_coverage() -> float:
    """Run existing tests and return the total coverage percentage."""
    cmd = [
        sys.executable, "-m", "pytest",
        str(TESTS_DIR), "--tb=short",
        f"--cov={REPO_ROOT / 'src' / 'cocapn_plato'}",
        "--cov-report=json", "-q",
    ]
    try:
        subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
        )
    except subprocess.TimeoutExpired:
        pass
    return get_coverage_pct(COVERAGE_JSON)


def validate_all_generated(
    generated_dir: str | Path | None = None,
    baseline_coverage: float | None = None,
) -> list[ValidationResult]:
    """Validate all generated test files in the generated_tests/ directory."""
    gen_dir = Path(generated_dir) if generated_dir else GEN_DIR
    if not gen_dir.exists():
        return []

    if baseline_coverage is None:
        baseline_coverage = _compute_baseline_coverage()

    results: list[ValidationResult] = []
    for py_file in sorted(gen_dir.glob("test_*.py")):
        result = run_with_coverage(py_file, baseline_coverage)
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Validate generated test candidates.")
    parser.add_argument("input", nargs="?", help="Path to LLM response text file (optional)")
    parser.add_argument("--baseline", type=float, default=None, help="Baseline coverage %% (computed if omitted)")
    parser.add_argument("--all", action="store_true", dest="validate_all", help="Validate all files in generated_tests/")
    parser.add_argument("--dir", default=str(GEN_DIR), help="Directory with generated tests")
    parser.add_argument("--json", "-o", default="validation_results.json", help="Output JSON path")
    parser.add_argument("--skip-run", action="store_true", help="Only write files, do not run tests")
    args = parser.parse_args()

    if args.validate_all:
        results = validate_all_generated(args.dir, args.baseline)
    elif args.input:
        text = Path(args.input).read_text(encoding="utf-8")
        results = validate_llm_output(text, args.baseline, auto_run=not args.skip_run)
    else:
        parser.print_help()
        sys.exit(1)

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "coverage_delta_total": sum(r.coverage_delta for r in results if r.passed),
        "results": [
            {
                "passed": r.passed,
                "coverage_delta": r.coverage_delta,
                "error_message": r.error_message,
                "test_file": r.test_file,
            }
            for r in results
        ],
    }

    out_path = Path(args.json)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote validation summary to {out_path}")

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        delta = f"+{r.coverage_delta:.2f}%" if r.passed else f"{r.coverage_delta:.2f}%"
        print(f"  [{status}] {r.test_file or '?'}  ({delta})")
        if r.error_message and not r.passed:
            print(f"         → {r.error_message[:200]}")

    if summary["passed"] == 0 and summary["total"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    _main()
