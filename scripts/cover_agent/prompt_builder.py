"""Prompt builder for Cover-Agent test generation.

Constructs LLM prompts that include:
- Source code context (focal method + surrounding lines)
- Existing test patterns from the repo
- Coverage gap information (which lines are untested)
- Specific instructions: "Test error paths, edge cases, and boundary conditions"

Usage:
    from scripts.cover_agent.prompt_builder import PromptBuilder
    builder = PromptBuilder(repo_root=Path("."))
    prompt = builder.build(module="cocapn_plato.engine.monitor", ...)
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("cover_agent.prompt_builder")

# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """\
You are a senior Python test engineer working on the Cocapn Fleet PLATO project.

RULES:
1. Output ONLY raw Python pytest code. No markdown fences. No explanations.
2. Every test must be a standalone function named `test_*` in a single file.
3. Use `pytest` conventions. Import the module under test with:
   `from cocapn_plato.<module> import <ClassOrFunction>`
4. Test error paths, edge cases, and boundary conditions — not just happy paths.
5. Mock external I/O (HTTP, files, databases) with `unittest.mock` or `pytest.MonkeyPatch`.
6. Do NOT write docstrings inside test functions. Comments OK for complex setups.
7. Use `assert` statements. Keep tests concise but thorough.
8. If the module is async, use `pytest.mark.asyncio` + `async def`.
9. If testing CLI output, capture `sys.stdout` / `sys.stderr` or use `click.testing.CliRunner` if click is used.
10. Do NOT import or reference any test file that does not exist in the repo.
"""

PROMPT_TEMPLATE = """\
{system_instructions}

---
MODULE UNDER TEST: {module}
FILE: {file_path}
CURRENT COVERAGE: {coverage:.1f}%
UNTESTED LINES: {missing_lines}

---
SOURCE CODE (full file):
```python
{source_code}
```

---
EXISTING TEST PATTERNS FROM REPO:
{test_patterns}

---
TASK:
Write pytest tests that cover the MISSING lines listed above.
Focus on:
- Error paths and exception handling
- Edge cases and boundary conditions
- State transitions and side effects
- Any async paths

Produce a SINGLE Python file with all necessary test functions.
"""

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------


class PromptBuilder:
    """Constructs prompts for LLM-based test generation."""

    def __init__(self, repo_root: Path, max_source_lines: int = 300):
        self.repo_root = Path(repo_root)
        self.max_source_lines = max_source_lines
        self._test_cache: dict[str, str] = {}

    def build(
        self,
        module: str,
        source_path: Path,
        missing_lines: list[int],
        existing_tests_dir: Path,
    ) -> str:
        """Assemble a complete prompt for the target module."""
        source_code = self._load_source(source_path)
        test_patterns = self._collect_test_patterns(module, existing_tests_dir)
        coverage = self._estimate_coverage(source_path)

        # Summarize missing lines compactly
        missing_summary = self._summarize_missing(missing_lines)

        prompt = PROMPT_TEMPLATE.format(
            system_instructions=SYSTEM_INSTRUCTIONS,
            module=module,
            file_path=str(source_path.relative_to(self.repo_root)),
            coverage=coverage,
            missing_lines=missing_summary,
            source_code=source_code,
            test_patterns=test_patterns,
        )
        return prompt

    def _load_source(self, path: Path) -> str:
        """Read source file, truncated if too long."""
        try:
            with open(path) as f:
                lines = f.readlines()
        except Exception as exc:
            logger.warning("Cannot read %s: %s", path, exc)
            return "# [source unavailable]"

        if len(lines) > self.max_source_lines:
            # Keep header + first chunk + footer
            head = lines[: self.max_source_lines // 2]
            tail = lines[-self.max_source_lines // 4 :]
            lines = head + ["\n# ... [truncated for length] ...\n\n"] + tail
        return "".join(lines)

    def _collect_test_patterns(self, module: str, tests_dir: Path) -> str:
        """Find existing test files related to the module and return excerpts."""
        # Map module to likely test file name
        short = module.split(".")[-1]  # e.g., "monitor"
        candidates = [
            tests_dir / f"test_{short}.py",
            tests_dir / f"test_{module.replace('.', '_')}.py",
        ]

        # Also scan all test files for imports of this module
        for test_file in sorted(tests_dir.glob("test_*.py")):
            if test_file in candidates:
                continue
            try:
                with open(test_file) as f:
                    content = f.read()
                if module in content or short in content:
                    candidates.append(test_file)
            except Exception:
                pass

        excerpts: list[str] = []
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                with open(candidate) as f:
                    content = f.read()
                # Include imports and first few test functions
                excerpt = self._extract_excerpt(content, max_lines=80)
                excerpts.append(
                    f"--- From {candidate.name} ---\n{excerpt}\n"
                )
            except Exception as exc:
                logger.debug("Skipping %s: %s", candidate, exc)

        if not excerpts:
            return "(No existing test patterns found for this module.)"
        return "\n".join(excerpts)

    def _extract_excerpt(self, content: str, max_lines: int = 80) -> str:
        """Return the import section + first N lines of test code."""
        lines = content.splitlines()
        # Find first def/test
        first_test = 0
        for i, line in enumerate(lines):
            if line.strip().startswith(("def test_", "class Test")):
                first_test = i
                break
        # Include imports + 2 test functions roughly
        end = first_test
        func_count = 0
        for i in range(first_test, len(lines)):
            if lines[i].strip().startswith(("def test_", "class Test")):
                func_count += 1
                if func_count > 2:
                    break
            end = i
        excerpt_lines = lines[: min(end + 1, max_lines)]
        if len(lines) > max_lines:
            excerpt_lines.append("# ... [truncated] ...")
        return "\n".join(excerpt_lines)

    def _summarize_missing(self, missing_lines: list[int]) -> str:
        """Compress missing line numbers into readable ranges."""
        if not missing_lines:
            return "None"
        ranges: list[str] = []
        start = missing_lines[0]
        prev = start
        for line in missing_lines[1:]:
            if line == prev + 1:
                prev = line
            else:
                ranges.append(f"{start}" if start == prev else f"{start}-{prev}")
                start = prev = line
        ranges.append(f"{start}" if start == prev else f"{start}-{prev}")
        return ", ".join(ranges[:10]) + (" ..." if len(ranges) > 10 else "")

    def _estimate_coverage(self, source_path: Path) -> float:
        """Read coverage.json for this file's coverage percentage."""
        coverage_json = self.repo_root / "coverage.json"
        if not coverage_json.exists():
            return 0.0
        import json

        with open(coverage_json) as f:
            data = json.load(f)
        rel = str(source_path.relative_to(self.repo_root))
        info = data.get("files", {}).get(rel, {})
        return info.get("summary", {}).get("percent_covered", 0.0)
