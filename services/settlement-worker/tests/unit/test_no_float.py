"""P4 for Python (D-093): no float can touch money in the worker.

An AST scan of settlement_worker/ fails on a float literal, a call to float(), or
parse_float=float. Money is Decimal, from asyncpg NUMERIC or from validated strings.
"""

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "src" / "settlement_worker"


def _violations(tree: ast.AST, path: Path) -> list[str]:
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, float):
            found.append(f"{path.name}:{node.lineno} float literal {node.value!r}")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "float":
            found.append(f"{path.name}:{node.lineno} float() call")
        elif (
            isinstance(node, ast.keyword)
            and node.arg == "parse_float"
            and isinstance(node.value, ast.Name)
            and node.value.id == "float"
        ):
            found.append(f"{path.name}:{node.value.lineno} parse_float=float")
    return found


def test_no_float_in_the_worker():
    files = sorted(SOURCE.rglob("*.py"))
    assert files, f"no sources under {SOURCE}"
    violations = [v for path in files for v in _violations(ast.parse(path.read_text(encoding="utf-8")), path)]
    assert violations == []


def test_the_scan_finds_each_form():
    source = "x = 0.5\ny = float('1')\nimport json\njson.loads('1', parse_float=float)\n"
    assert len(_violations(ast.parse(source), Path("probe.py"))) == 3
