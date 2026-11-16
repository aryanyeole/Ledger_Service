"""The worker's Redis client settings (D-137): no retries, the contract's timeouts, database 0.

redis-py 8.1.0's default is Retry(ExponentialWithJitterBackoff(base=0.01, cap=1), retries=10) on ConnectionError
and TimeoutError (redis/_defaults.py, redis/asyncio/client.py), so the factory's explicit retry is checked on
the client, on the pool's connection settings and on a connection the pool would build. The behaviour against a
closed port is in tests/smoke/test_redis_smoke.py.
"""

from redis.backoff import NoBackoff

from settlement_worker import redis_client


def test_client_and_its_connections_never_retry():
    client = redis_client.create("redis://127.0.0.1:6390")
    pool = client.connection_pool
    connection = pool.connection_class(**pool.connection_kwargs)
    for retry in (client.get_retry(), pool.connection_kwargs["retry"], connection.retry):
        assert retry.get_retries() == 0
        assert isinstance(retry._backoff, NoBackoff)


def test_timeouts_database_and_address():
    client = redis_client.create("redis://127.0.0.1:6390/0")
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == redis_client.COMMAND_TIMEOUT_MS / 1000
    assert kwargs["socket_connect_timeout"] == redis_client.CONNECT_TIMEOUT_MS / 1000
    assert (kwargs["host"], kwargs["port"], kwargs["db"]) == ("127.0.0.1", 6390, 0)
    assert (redis_client.COMMAND_TIMEOUT_MS, redis_client.CONNECT_TIMEOUT_MS) == (100, 200)
