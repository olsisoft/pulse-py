"""B-119 Track B.3 (decision D3) — real compile/exec check of the Developer
Hub's Python snippets against the installed ``pulse_client`` SDK.

The structural lint on the JVM side (``DeveloperHubSnippetLintTest``) guards
fence tags + connector-type names. This is the stronger Python slice: it
proves the recipes actually run against the real SDK, so a renamed DSL method
or a wrong argument shape (e.g. ``aggregations=[...]`` instead of a dict) fails
CI instead of shipping a recipe that throws when a developer copies it.

Two layers:
  1. **Syntax** — every ```python block must ``ast.parse``.
  2. **Execution** — every *synchronous* snippet that builds a ``StreamBuilder``
     is exec'd with the REAL ``StreamBuilder`` / ``windows`` / ``aggs`` and a
     mocked network client. The real builder validates its own arguments, so a
     bad ``window(...)`` shape raises here.

Async snippets (``async with``/``await`` — duplex, SSE consumers) are
syntax-checked only; exercising them needs an event loop + async mocks and the
DSL-shape risk doesn't live there.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest import mock

import pytest

from pulse_client import PulseClient, StreamBuilder, WindowSpec, aggs, windows  # noqa: F401

# Repo-relative location of the JAR-bundled Developer Hub Markdown.
_DOCS_DIR = (
    Path(__file__).resolve().parents[2]
    / "streamflow-pulse"
    / "src"
    / "main"
    / "resources"
    / "developer-hub"
)

_PYTHON_BLOCK = re.compile(r"```python\n(.*?)\n```", re.DOTALL)


def _python_snippets() -> list[tuple[str, int, str]]:
    """(filename, 1-based block index, code) for every python fence."""
    if not _DOCS_DIR.is_dir():
        pytest.skip(f"developer-hub docs not found at {_DOCS_DIR}", allow_module_level=True)
    out: list[tuple[str, int, str]] = []
    for md in sorted(_DOCS_DIR.glob("*.md")):
        for i, m in enumerate(_PYTHON_BLOCK.finditer(md.read_text(encoding="utf-8")), 1):
            out.append((md.name, i, m.group(1)))
    return out


def _exec_dsl(code: str) -> None:
    """Exec a synchronous DSL snippet with the real builder + a mocked client.

    Undefined names (``client``, ``schema``, ``definition``, …) are supplied as
    ``MagicMock`` on demand via NameError-retry, so the only *real* objects
    exercised are ``StreamBuilder`` / ``windows`` / ``aggs`` — which is exactly
    what we want to validate.
    """
    ns: dict[str, object] = {
        "StreamBuilder": StreamBuilder,
        "windows": windows,
        "aggs": aggs,
        "WindowSpec": WindowSpec,
        "PulseClient": mock.MagicMock(name="PulseClient"),
    }
    compiled = compile(code, "<snippet>", "exec")
    for _ in range(64):  # bounded NameError-retry to inject mock placeholders
        try:
            exec(compiled, ns)  # noqa: S102 — trusted in-repo doc snippets
            return
        except NameError as e:
            missing = str(e).split("'")[1]
            ns[missing] = mock.MagicMock(name=missing)
    raise AssertionError("snippet needed more than 64 placeholder names")


SNIPPETS = _python_snippets()


def test_developer_hub_has_python_snippets() -> None:
    assert SNIPPETS, "expected python snippets in the developer-hub docs"


@pytest.mark.parametrize("name,idx,code", SNIPPETS, ids=[f"{n}#{i}" for n, i, _ in SNIPPETS])
def test_snippet_parses(name: str, idx: int, code: str) -> None:
    try:
        ast.parse(code)
    except SyntaxError as e:  # pragma: no cover - failure path
        pytest.fail(f"{name} block #{idx} has a syntax error: {e}")


_DSL = [(n, i, c) for (n, i, c) in SNIPPETS
        if "StreamBuilder(" in c and "await " not in c and "async " not in c]


@pytest.mark.parametrize("name,idx,code", _DSL, ids=[f"{n}#{i}" for n, i, _ in _DSL])
def test_dsl_snippet_executes_against_real_sdk(name: str, idx: int, code: str) -> None:
    try:
        _exec_dsl(code)
    except Exception as e:  # noqa: BLE001 - surface as a test failure
        pytest.fail(f"{name} block #{idx} failed against the real SDK: {type(e).__name__}: {e}")
