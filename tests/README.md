# Tests Layout

This repository keeps tests in a top-level `tests/` directory so test code is separated from importable package code.

## Structure

- `tests/rewrite/`: tests for deployment and rewrite orchestration logic

## Conventions

- Name files as `test_*.py`.
- Prefer behavior-focused tests that import package modules the same way users do.
- Keep fixtures and stubs local to the test module unless they are reused in multiple files.

## Running Tests

Run all configured tests:

```bash
python -m pytest
```

Run rewrite tests only:

```bash
python -m pytest tests/rewrite
```

If your environment does not include optional pytest plugins configured in `pyproject.toml`, you can temporarily override addopts:

```bash
python -m pytest -o addopts='' tests/rewrite
```
