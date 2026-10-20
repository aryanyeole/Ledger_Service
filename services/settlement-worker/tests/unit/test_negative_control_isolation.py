"""D-017: main code never imports a negative control.

An AST scan of settlement_worker/ fails on any import of tests or negative_controls, whatever the
form (import x, from x import y, relative or absolute).
"""

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "src" / "settlement_worker"
FORBIDDEN = ("negative_controls", "tests")


def _imports(tree: ast.AST) -> list[str]:
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def test_main_never_imports_a_negative_control():
    files = sorted(SOURCE.rglob("*.py"))
    assert files, f"no sources under {SOURCE}"
    found = [
        f"{path.name}: import {name}"
        for path in files
        for name in _imports(ast.parse(path.read_text(encoding="utf-8")))
        if any(part in FORBIDDEN for part in name.split("."))
    ]
    assert found == []
