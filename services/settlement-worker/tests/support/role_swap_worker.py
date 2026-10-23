"""The real worker, whose reconnects see the wrong role (D-129). TEST SOURCES ONLY; main never imports it.

It reads the real Config from the environment, then copies it into a test-only subclass whose
db_params() connects as settlement_worker the first time and as ledger_api_app (password from
ROLE_SWAP_PASSWORD) on every later connect, and runs the real consumer.run(). Startup passes the
role check; after its connection is lost, the reconnect's role check refuses it (exit 2). No
runtime role can change, so only a test-side config can make a reconnect see the wrong role.

Launched from tests/ with: python -m support.role_swap_worker
"""

import asyncio
import dataclasses
import os

import structlog

from settlement_worker import consumer, events, logging_setup
from settlement_worker.config import Config, ConfigError

SWAPPED_ROLE = "ledger_api_app"


@dataclasses.dataclass(frozen=True)
class RoleSwapConfig(Config):
    swap_password: str = dataclasses.field(default="", repr=False)
    connects: list = dataclasses.field(default_factory=lambda: [0], repr=False, compare=False)

    def db_params(self) -> dict:
        self.connects[0] += 1
        params = super().db_params()
        if self.connects[0] == 1:
            return params
        return {**params, "user": SWAPPED_ROLE, "password": self.swap_password}


async def _run(config: Config) -> int:
    return await consumer.run(config, asyncio.Event(), {})


def main() -> int:
    logging_setup.configure()
    try:
        base = Config.from_env()
    except ConfigError as ex:
        events.emit(structlog.get_logger(), events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return consumer.EXIT_CONFIG
    config = RoleSwapConfig(
        **{f.name: getattr(base, f.name) for f in dataclasses.fields(Config)},
        swap_password=os.environ.get("ROLE_SWAP_PASSWORD", ""),
    )
    return asyncio.run(_run(config))


if __name__ == "__main__":
    raise SystemExit(main())
