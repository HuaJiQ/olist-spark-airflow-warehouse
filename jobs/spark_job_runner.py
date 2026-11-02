
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

# 只允许执行登记过的作业
ALLOWED_JOBS = {
    "olist_dws_smoke": "/workspace/jobs/m6_spark_smoke.py"
}

run_lock = threading.Lock()


def execute_job(job_name):
    if job_name not in ALLOWED_JOBS:
        raise ValueError("Job is not allowed")

    command = [
        SPARK_SUBMIT,
        "--master", "local[2]",
        ALLOWED_JOBS[job_name],
    ]

    print(f"[RUNNER] Starting {job_name}", flush=True)

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )

    print(result.stdout[-4000:], flush=True)
    print(result.stderr[-4000:], flush=True)

    return {
        "job": job_name,
        "returncode": result.returncode,
        "success": result.returncode == 0,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }


class Handler(BaseHTTPRequestHandler):

    def respond(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.respond(200, {"status": "ok"})
        else:
            self.respond(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/run":
            self.respond(404, {"error": "Not found"})
            return

        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 1024:
                self.respond(400, {"error": "Invalid body size"})
                return

            request = json.loads(
                self.rfile.read(size).decode("utf-8")
            )

            job_name = request.get("job")

            if job_name not in ALLOWED_JOBS:
                self.respond(400, {"error": "Unknown job"})
                return

            if not run_lock.acquire(blocking=False):
                self.respond(409, {"error": "Runner is busy"})
                return

            try:
                result = execute_job(job_name)
            finally:
                run_lock.release()

            self.respond(
                200 if result["success"] else 500,
                result
            )

        except subprocess.TimeoutExpired:
            self.respond(504, {"error": "Spark job timed out"})

        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.respond(400, {"error": str(exc)})

        except Exception as exc:
            print(f"[RUNNER ERROR] {exc}", flush=True)
            self.respond(500, {"error": "Runner failed"})


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8765), Handler)
    print("[RUNNER] Listening on port 8765", flush=True)
    server.serve_forever()
