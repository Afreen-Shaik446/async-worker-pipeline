"""Tiny HTTP health endpoint (stdlib only) reporting broker/store connectivity."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_handler(consumer):
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/healthz":
                self.send_response(404)
                self.end_headers()
                return
            checks = {"status": "ok"}
            try:
                consumer.mongo.admin.command("ping")
                checks["mongodb"] = "ok"
            except Exception as exc:  # noqa: BLE001
                checks["mongodb"] = f"error: {exc}"
                checks["status"] = "degraded"
            try:
                checks["rabbitmq"] = (
                    "ok" if consumer.connection.is_open else "connection closed"
                )
                if not consumer.connection.is_open:
                    checks["status"] = "degraded"
            except Exception as exc:  # noqa: BLE001
                checks["rabbitmq"] = f"error: {exc}"
                checks["status"] = "degraded"

            body = json.dumps(checks).encode()
            status = 200 if checks["status"] == "ok" else 503
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep logs quiet
            pass

    return HealthHandler


def start_health_server(port: int, consumer) -> None:
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(consumer))
    server.serve_forever()
