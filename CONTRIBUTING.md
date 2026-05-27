# Contributing to cocapn-plato

Thanks for helping the Cocapn Fleet. Every claw counts.

## Quick Start

```bash
git clone https://github.com/SuperInstance/cocapn-plato.git
cd cocapn-plato
make install
make test
```

## Development Workflow

1. **Branch:** `git checkout -b feature/your-thing`
2. **Code:** Follow PEP 8, keep lines ≤ 100 chars where reasonable
3. **Test:** `make test` — all 36+ tests must pass
4. **Lint:** `make lint` — ruff + mypy (mypy allowed to warn, not block)
5. **Coverage:** `make coverage` — target is 75% (currently ~37%, needs server + SDK tests)
6. **Commit:** Clear message, reference issue if any
7. **Push:** `git push origin feature/your-thing`
8. **PR:** Against `main`, CI must go green

## Code Style

- **Formatter:** ruff (replaces black + isort + flake8)
- **Type hints:** Encouraged, not enforced (mypy is `|| true` in CI)
- **Docstrings:** For public APIs; Google or plain style, be consistent
- **No:** `print()` in library code; use `logging`

## Testing

- Framework: pytest
- Run single file: `pytest tests/test_query.py -v`
- With coverage: `make coverage`
- Benchmarks: `pytest tests/test_benchmark.py -v` (10K stress tests)

## Security

- Never commit secrets, tokens, or private keys
- Bandit runs on every push and PR
- If pip-audit flags a dependency, open an issue immediately

## Release Process (maintainers only)

1. Bump version in `pyproject.toml`
2. Tag: `git tag -a v3.x.x -m "Release 3.x.x"`
3. Push tag: `git push origin v3.x.x`
4. CI builds, tests, and publishes to PyPI + GitHub Release

## Questions?

Open an issue or ping `#cocapn-build` on Matrix.
