"""The worker's /metrics endpoint (D-113).

prometheus_client's own HTTP server, on its default registry, in a daemon thread. Application
metrics are module-level collectors on that registry: RETRIES (D-108). The host and
port reported are the ones the server actually bound (server_address), so port 0 works. Any OSError
while binding, an unresolvable host included, is a MetricsBindError: the worker treats it as
invalid configuration (exit 2).
"""

from dataclasses import dataclass
from wsgiref.simple_server import WSGIServer

from prometheus_client import Counter, start_http_server

# Exposed as settlement_retries_total; sqlstate is "" when the failure has none.
RETRIES = Counter("settlement_retries", "Records scheduled for a retry in place (D-108)", ["error_class", "sqlstate"])


class MetricsBindError(RuntimeError):
    pass


@dataclass(frozen=True)
class MetricsServer:
    server: WSGIServer

    @property
    def host(self) -> str:
        return self.server.server_address[0]

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def close(self) -> None:
        """Stops serving (returns once the serve loop has exited) and closes the socket."""
        self.server.shutdown()
        self.server.server_close()


def start(host: str, port: int) -> MetricsServer:
    try:
        server, _thread = start_http_server(port, addr=host)
    except OSError as ex:
        raise MetricsBindError(f"cannot serve metrics on {host}:{port}: {ex}") from ex
    return MetricsServer(server)
