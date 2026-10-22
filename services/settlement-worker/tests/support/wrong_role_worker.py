"""The real worker, connecting as the wrong role (D-113). TEST SOURCES ONLY; main never imports it.

It reads the real Config from the environment, then copies it into a test-only subclass whose
db_params() connects as ledger_api_app (password from WRONG_ROLE_PASSWORD), and runs the real
consumer.run(): the real role check (db.connect) refuses the connection, and the run's startup order
decides whether a metrics port was opened first. Nothing is mocked; the database is real.

Launched from tests/ with: python -m support.wrong_role_worker
"""

import asyncio
import dataclasses
import os

import structlog

from settlement_worker import consumer, events, logging_setup
from settlement_worker.config import Config, ConfigError

WRONG_ROLE = "ledger_api_app"


@dataclasses.dataclass(frozen=True)
class WrongRoleConfig(Config):
    wrong_role_password: str = dataclasses.field(default="", repr=False)

    def db_params(self) -> dict:
        return {**super().db_params(), "user": WRONG_ROLE, "password": self.wrong_role_password}


async def _run(config: Config) -> int:
    return await consumer.run(config, asyncio.Event(), {})


def main() -> int:
    logging_setup.configure()
    try:
        base = Config.from_env()
    except ConfigError as ex:
        events.emit(structlog.get_logger(), events.CONFIG_INVALID, level="error", error=str(ex), correlation_id=None)
        return consumer.EXIT_CONFIG
    config = WrongRoleConfig(
        **{f.name: getattr(base, f.name) for f in dataclasses.fields(Config)},
        wrong_role_password=os.environ.get("WRONG_ROLE_PASSWORD", ""),
    )
    return asyncio.run(_run(config))


if __name__ == "__main__":
    raise SystemExit(main())
