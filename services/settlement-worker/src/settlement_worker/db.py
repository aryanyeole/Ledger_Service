"""The worker's Postgres connection (D-082, mirroring D-036 on the Java side).

The worker refuses to run unless it is connected as settlement_worker and that role is not a
superuser: its grants are half of the ledger's defences (D-033), and a superuser would bypass them.
"""

import asyncpg

ROLE = "settlement_worker"


class WorkerRoleError(RuntimeError):
    pass


async def connect(**params) -> asyncpg.Connection:
    conn = await asyncpg.connect(**params)
    try:
        row = await conn.fetchrow(
            "SELECT current_user AS who, rolsuper FROM pg_roles WHERE rolname = current_user"
        )
        if row is None or row["who"] != ROLE:
            raise WorkerRoleError(f"connected as {row['who'] if row else '?'}, expected {ROLE}")
        if row["rolsuper"]:
            raise WorkerRoleError(f"{ROLE} must not be a superuser")
    except BaseException:
        await conn.close()
        raise
    return conn
