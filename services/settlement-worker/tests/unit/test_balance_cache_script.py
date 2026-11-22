"""D-143: the worker's invalidation script is the contract text, byte for byte.

The text between <!-- script:invalidate --> and <!-- /script:invalidate --> in docs/design/cache-invalidation.md
§2.1 is the single copy both services are pinned against; ledger-api's BalanceCacheScriptsTest reads the same
markers. Line endings are normalized to \\n (a Windows checkout has CRLF).
"""

import re
from pathlib import Path

from settlement_worker import balance_cache

DOC = Path(__file__).resolve().parents[4] / "docs" / "design" / "cache-invalidation.md"


def _from_doc(name: str) -> str:
    doc = DOC.read_text(encoding="utf-8").replace("\r\n", "\n")
    blocks = re.findall(rf"<!-- script:{name} -->\n```lua\n(.*?)```\n<!-- /script:{name} -->", doc, re.DOTALL)
    assert len(blocks) == 1, f"exactly one script:{name} block in {DOC}, found {len(blocks)}"
    return blocks[0]


def test_invalidate_is_the_doc_text():
    assert balance_cache.INVALIDATE == _from_doc("invalidate")


def test_invalidate_is_the_literal_contract_text():
    assert balance_cache.INVALIDATE == (
        "-- ledger:v1 balance invalidation (D-142). KEYS: ledger:v1:bal:{id}, ledger:v1:bal-gen:{id} per account.\n"
        "-- ARGV[1]: a fresh token for this call.\n"
        "for i = 1, #KEYS, 2 do\n"
        "  redis.call('DEL', KEYS[i])\n"
        "  redis.call('SET', KEYS[i + 1], ARGV[1], 'PX', 300000)\n"
        "end\n"
        "return #KEYS\n"
    )
