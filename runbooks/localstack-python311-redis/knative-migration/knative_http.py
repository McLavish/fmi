#!/usr/bin/env python3
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lambda_function import lambda_handler


class InvokeHandler(BaseHTTPRequestHandler):
    server_version = "fmi-knative/1.0"

    def do_POST(self):
        if self.path != "/invoke":
            self.send_error(404)
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            result = lambda_handler(payload, None)
            self._write_json(result)
        except Exception as exc:
            self._write_json({"status": f"error: {exc}"}, status=500)

    def log_message(self, fmt, *args):
        print(f"[http] {self.address_string()} - {fmt % args}", flush=True)

    def _write_json(self, body, status=200):
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), InvokeHandler)
    print(f"[http] listening on :{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
