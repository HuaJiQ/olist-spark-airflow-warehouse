import re
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

# 只允许执行登记过的作业
ALLOWED_JOBS = {
    "olist_dws_smoke":
        "/workspace/jobs/m6_spark_smoke.py",

    "build_daily_sales":
        "/workspace/src/transformations/build_ads.py",

    "build_customer_ads":
        "/workspace/src/transformations/build_ads_customer.py",

    "build_category_sales":
        "/workspace/src/transformations/build_ads_category.py",
}
BATCH_JOBS = {
        "build_daily_sales",
        "build_customer_ads",
        "build_category_sales",
    }
run_lock = threading.Lock()

def execute_job(job_name, batch_id=None):
    if job_name not in ALLOWED_JOBS:
        raise ValueError("Job is not allowed")

    command = [
        SPARK_SUBMIT,
        "--master", "local[2]",
        ALLOWED_JOBS[job_name],
    ]

    if job_name in BATCH_JOBS:
        if not isinstance(batch_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,64}", batch_id
        ):
            raise ValueError("Invalid batch_id")

        command.extend(["--batch-id", batch_id])

    elif batch_id is not None:
        raise ValueError("This job does not accept batch_id")
        if not isinstance(batch_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,64}", batch_id
        ):
            raise ValueError("Invalid batch_id")

        command.extend(["--batch-id", batch_id])

    elif batch_id is not None:
        raise ValueError("This job does not accept batch_id")

    print(
        f"[RUNNER] Starting job={job_name}, batch_id={batch_id}",
        flush=True
    )

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )

    print(result.stdout[-4000:], flush=True)
    print(result.stderr[-4000:], flush=True)

    return {
        "job": job_name,
        "batch_id": batch_id,
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

            if not isinstance(request, dict):
                raise ValueError("Request must be a JSON object")

            job_name = request.get("job")
            batch_id = request.get("batch_id")

            if job_name in BATCH_JOBS:
                if not isinstance(batch_id, str) or not re.fullmatch(
                    r"[A-Za-z0-9_-]{1,64}", batch_id
                ):
                    raise ValueError("Invalid batch_id")

            elif batch_id is not None:
                raise ValueError("Unexpected batch_id")
                if not isinstance(batch_id, str) or not re.fullmatch(
                    r"[A-Za-z0-9_-]{1,64}", batch_id
                ):
                    raise ValueError("Invalid batch_id")
            elif batch_id is not None:
                raise ValueError("Unexpected batch_id")

            if job_name not in ALLOWED_JOBS:
                self.respond(400, {"error": "Unknown job"})
                return

            if not run_lock.acquire(blocking=False):
                self.respond(409, {"error": "Runner is busy"})
                return

            try:
                result = execute_job(job_name, batch_id)
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
