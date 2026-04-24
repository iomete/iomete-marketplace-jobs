import os
import argparse
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection
from requests import Response, Session
from requests.exceptions import RequestException


RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# This is the backend compute API path version used by this script.
COMPUTE_API_PATH_VERSION = "v2"


@dataclass(frozen=True)
class Cluster:
    """Minimal cluster data needed throughout the restart workflow."""

    compute_id: str
    domain: str
    namespace: str
    name: str
    driver_status: str


@dataclass(frozen=True)
class PollConfig:
    """Polling behavior for one lifecycle phase such as STOP or START."""

    base_interval_seconds: float
    max_interval_seconds: float
    timeout_seconds: float
    backoff_multiplier: float


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded once at startup."""

    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str
    api_base_url: str
    api_token: str
    request_timeout: int
    dry_run: bool
    stop_poll: PollConfig
    start_poll: PollConfig
    api_retry_count: int
    api_retry_delay_seconds: int
    logs_dir: Path


# ---------------------------
# Environment / config helpers
# ---------------------------


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def get_float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def load_config(env_file: str = ".env") -> Config:
    """
    Load everything once so the rest of the script reads from a single config object.
    That keeps env parsing out of the operational logic.
    """
    load_dotenv(dotenv_path=env_file)

    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Start with short polls so fast transitions complete quickly.
    # Backoff and max interval reduce API noise when a cluster takes longer.
    stop_base = get_float_env("STOP_POLL_INTERVAL_SECONDS", 2.0)
    start_base = get_float_env("START_POLL_INTERVAL_SECONDS", stop_base)

    stop_poll = PollConfig(
        base_interval_seconds=stop_base,
        max_interval_seconds=get_float_env("STOP_POLL_MAX_INTERVAL_SECONDS", 12.0),
        timeout_seconds=get_float_env("STOP_POLL_TIMEOUT_SECONDS", 90.0),
        backoff_multiplier=get_float_env("STOP_POLL_BACKOFF_MULTIPLIER", 1.5),
    )
    start_poll = PollConfig(
        base_interval_seconds=start_base,
        max_interval_seconds=get_float_env("START_POLL_MAX_INTERVAL_SECONDS", 12.0),
        timeout_seconds=get_float_env("START_POLL_TIMEOUT_SECONDS", 120.0),
        backoff_multiplier=get_float_env("START_POLL_BACKOFF_MULTIPLIER", 1.5),
    )

    return Config(
        db_host=get_env("DB_HOST"),
        db_port=get_env("DB_PORT"),
        db_name=get_env("DB_NAME"),
        db_user=get_env("DB_USER"),
        db_password=get_env("DB_PASSWORD"),
        api_base_url=get_env("API_BASE_URL").rstrip("/"),
        api_token=get_env("API_TOKEN"),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
        dry_run=get_bool_env("DRY_RUN", default=True),
        stop_poll=stop_poll,
        start_poll=start_poll,
        api_retry_count=int(os.getenv("API_RETRY_COUNT", "1")),
        api_retry_delay_seconds=int(os.getenv("API_RETRY_DELAY_SECONDS", "2")),
        logs_dir=logs_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restart active compute clusters using the configured environment."
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file to load. Defaults to .env",
    )
    return parser.parse_args()


# --------
# Logging
# --------


def _write_log(message: str, log_file: Path, color: str | None = None) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"

    if color:
        print(f"{color}{line}{RESET}")
    else:
        print(line)

    with log_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def log(message: str, log_file: Path) -> None:
    _write_log(message, log_file)


def log_warning(message: str, log_file: Path) -> None:
    _write_log(message, log_file, color=YELLOW)


def log_error(message: str, log_file: Path) -> None:
    _write_log(message, log_file, color=RED)


# ----------------
# Database helpers
# ----------------


def open_db_connection(config: Config) -> PgConnection:
    return psycopg2.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
    )


def fetch_active_clusters(config: Config) -> list[Cluster]:
    """
    Use the DB only to find restart candidates quickly.
    The API is still treated as the runtime source of truth during stop/start polling.
    """
    query = """
        SELECT id, domain, namespace, name, driver_status
        FROM lakehouse
        WHERE is_deleted = false
          AND driver_status = 'ACTIVE'
        ORDER BY created_at DESC;
    """

    with open_db_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [Cluster(*row) for row in rows]


# ---------------------
# Compute API operations
# ---------------------
class ComputeApiClient:
    """
    Light wrapper around the compute API.

    This keeps auth, URL building, and retry behavior in one place so the restart
    workflow reads more like business logic and less like HTTP plumbing.
    """

    def __init__(self, config: Config, log_file: Path):
        self.config = config
        self.log_file = log_file
        self.session = Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.api_token}",
                "Accept": "application/json",
            }
        )

    def close(self) -> None:
        self.session.close()

    def _compute_url(self, cluster: Cluster) -> str:
        return (
            f"{self.config.api_base_url}/api/{COMPUTE_API_PATH_VERSION}"
            f"/domains/{cluster.domain}/compute/{cluster.compute_id}"
        )

    def stop_url(self, cluster: Cluster) -> str:
        return f"{self._compute_url(cluster)}/stop"

    def start_url(self, cluster: Cluster) -> str:
        return f"{self._compute_url(cluster)}/start"

    def request_with_retry(self, method: str, url: str) -> Response:
        """
        Retry only transport/HTTP-layer failures.

        This is separate from the higher-level restart retry in main().
        Request retries handle flaky API calls; workflow retry handles a cluster
        that still failed to restart even though the HTTP requests themselves worked.
        """
        last_error: Exception | None = None

        for attempt in range(self.config.api_retry_count + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.config.request_timeout,
                )
                response.raise_for_status()
                return response
            except RequestException as exc:
                last_error = exc
                is_last_attempt = attempt == self.config.api_retry_count
                if not is_last_attempt:
                    log_warning(
                        f"{method.upper()} failed for {url}. Retry {attempt + 1}/{self.config.api_retry_count} "
                        f"after {self.config.api_retry_delay_seconds}s. Error: {exc}",
                        self.log_file,
                    )
                    time.sleep(self.config.api_retry_delay_seconds)

        raise RuntimeError(
            f"{method.upper()} failed after retries for {url}: {last_error}"
        )

    def get_compute_details(self, cluster: Cluster) -> dict[str, Any]:
        response = self.request_with_retry("GET", self._compute_url(cluster))
        return response.json()

    def stop_compute(self, cluster: Cluster) -> Response:
        return self.request_with_retry("POST", self.stop_url(cluster))

    def start_compute(self, cluster: Cluster) -> Response:
        return self.request_with_retry("POST", self.start_url(cluster))


# -----------------
# Polling / waiting
# -----------------


# Polling reads lifecycle state from the compute details API.
# Verified response field: driverStatus
def extract_driver_status(payload: dict[str, Any]) -> str | None:
    value = payload.get("driverStatus")
    return str(value) if value is not None else None


# The compute details API also exposes a driver-side error message.
# Include it in bad-state logs when available.
def extract_driver_error_message(payload: dict[str, Any]) -> str | None:
    value = payload.get("driverErrorMessage")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def wait_for_cluster_state(
    api: ComputeApiClient,
    cluster: Cluster,
    target_status: str,
    poll: PollConfig,
    phase_name: str,
) -> tuple[bool, str | None]:
    """
    Shared polling loop for both STOP and START.

    We start with a short interval so fast transitions complete quickly, then back
    off gradually to reduce API noise for slower clusters.
    """
    started_at = time.time()
    sleep_seconds = poll.base_interval_seconds
    bad_states = {"FAILED", "ERROR"}
    attempt = 1

    while True:
        payload = api.get_compute_details(cluster)
        status = extract_driver_status(payload)
        elapsed = time.time() - started_at

        log(
            f"[{phase_name}] Poll #{attempt} for {cluster.compute_id} in {cluster.domain}: "
            f"status={status}, elapsed={elapsed:.1f}s",
            api.log_file,
        )

        if status is None:
            log_warning(
                f"[{phase_name}] Could not determine driver status for {cluster.compute_id}.",
                api.log_file,
            )
            return False, status

        if status in bad_states:
            error_message = extract_driver_error_message(payload)
            if error_message:
                log_error(
                    f"[{phase_name}] Compute {cluster.compute_id} entered unhealthy state: "
                    f"{status}. Error: {error_message}",
                    api.log_file,
                )
            else:
                log_error(
                    f"[{phase_name}] Compute {cluster.compute_id} entered unhealthy state: {status}",
                    api.log_file,
                )
            return False, status

        if status == target_status:
            return True, status

        if elapsed >= poll.timeout_seconds:
            return False, status

        time.sleep(sleep_seconds)
        sleep_seconds = min(
            sleep_seconds * poll.backoff_multiplier, poll.max_interval_seconds
        )
        attempt += 1


# -----------------
# Restart workflow
# -----------------


def restart_cluster(api: ComputeApiClient, cluster: Cluster) -> None:
    """
    Restart one cluster safely:
    STOP -> wait for STOPPED -> START -> wait for ACTIVE.
    """
    log(
        f"Restarting cluster: {cluster.name} ({cluster.compute_id}) "
        f"in domain {cluster.domain}, namespace {cluster.namespace}",
        api.log_file,
    )

    log("Sending STOP request", api.log_file)
    stop_response = api.stop_compute(cluster)
    log(f"STOP response: {stop_response.status_code}", api.log_file)

    log("Waiting for compute to become STOPPED...", api.log_file)
    stopped_ok, stopped_status = wait_for_cluster_state(
        api=api,
        cluster=cluster,
        target_status="STOPPED",
        poll=api.config.stop_poll,
        phase_name="STOP",
    )
    if not stopped_ok:
        if stopped_status is None:
            raise RuntimeError("Could not determine compute status after STOP request.")
        raise RuntimeError(
            f"STOP phase did not reach STOPPED. Last observed status: {stopped_status}"
        )

    log(f"Compute reached STOPPED. Current status: {stopped_status}", api.log_file)
    log("Sending START request", api.log_file)
    start_response = api.start_compute(cluster)
    log(f"START response: {start_response.status_code}", api.log_file)

    log("Waiting for compute to become ACTIVE again...", api.log_file)
    started_ok, started_status = wait_for_cluster_state(
        api=api,
        cluster=cluster,
        target_status="ACTIVE",
        poll=api.config.start_poll,
        phase_name="START",
    )
    if not started_ok:
        if started_status is None:
            raise RuntimeError(
                "Could not determine compute status after START request."
            )
        raise RuntimeError(
            f"START phase did not reach ACTIVE. Last observed status: {started_status}"
        )

    log(
        f"Restart finished successfully. Current status: {started_status}", api.log_file
    )


def run_restart_pass(
    api: ComputeApiClient,
    clusters: list[Cluster],
    pass_name: str,
) -> tuple[list[Cluster], list[tuple[Cluster, str]]]:
    """
    Run one pass across a list of clusters.

    We keep this separate so PASS 1 and PASS 2 reuse the same behavior instead of
    duplicating the whole restart loop twice.
    """
    successes: list[Cluster] = []
    failures: list[tuple[Cluster, str]] = []

    for cluster in clusters:
        cluster_started_at = time.time()
        try:
            log(
                f"[{pass_name}] Working on {cluster.name} ({cluster.compute_id}) "
                f"in domain {cluster.domain}, namespace {cluster.namespace}",
                api.log_file,
            )
            restart_cluster(api, cluster)
            log(
                f"[{pass_name}] Restart completed in {time.time() - cluster_started_at:.2f}s",
                api.log_file,
            )
            successes.append(cluster)
        except Exception as exc:
            log_error(f"[{pass_name}] FAILED: {exc}", api.log_file)
            failures.append((cluster, str(exc)))

    return successes, failures


# --------------------
# Reporting / printing
# --------------------


def print_plan(clusters: list[Cluster], api: ComputeApiClient) -> None:
    log(f"Found {len(clusters)} active compute clusters", api.log_file)
    log("-" * 100, api.log_file)

    for cluster in clusters:
        log(f"name={cluster.name}", api.log_file)
        log(f"id={cluster.compute_id}", api.log_file)
        log(f"domain={cluster.domain}", api.log_file)
        log(f"namespace={cluster.namespace}", api.log_file)
        log(f"status={cluster.driver_status}", api.log_file)
        log(f"STOP URL -> {api.stop_url(cluster)}", api.log_file)
        log(f"START URL -> {api.start_url(cluster)}", api.log_file)
        log("-" * 100, api.log_file)


def write_failures_file(failures: list[tuple[Cluster, str]], logs_dir: Path) -> Path:
    failed_file = (
        logs_dir / f"failed_clusters_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    with failed_file.open("w", encoding="utf-8") as f:
        for cluster, error in failures:
            f.write(
                f"{cluster.compute_id},{cluster.domain},{cluster.namespace},{cluster.name},{error}\n"
            )
    return failed_file


def print_summary(
    clusters: list[Cluster],
    successes: list[Cluster],
    failures: list[tuple[Cluster, str]],
    log_file: Path,
    global_start: float,
    execution_start: float,
) -> None:
    log("=" * 100, log_file)
    log("RESTART SUMMARY", log_file)
    log("=" * 100, log_file)

    log(f"Successes: {len(successes)}", log_file)
    for cluster in successes:
        log(
            f"OK  | {cluster.name} | {cluster.domain} | {cluster.namespace} | {cluster.compute_id}",
            log_file,
        )

    log(f"Failures: {len(failures)}", log_file)
    for cluster, error in failures:
        log_error(
            f"ERR | {cluster.name} | {cluster.domain} | {cluster.namespace} | {cluster.compute_id}",
            log_file,
        )
        log_error(f"    {error}", log_file)

    success_rate = (len(successes) / len(clusters)) * 100 if clusters else 0
    total_runtime = time.time() - global_start
    execution_runtime = time.time() - execution_start

    log(f"Log file saved to: {log_file}", log_file)
    log(f"Success rate: {success_rate:.2f}%", log_file)
    log(
        f"Execution time (excluding confirmation wait): {execution_runtime:.2f}s",
        log_file,
    )
    log(f"Total script runtime: {total_runtime:.2f}s", log_file)
    log(f"Total targets: {len(clusters)}", log_file)
    log(f"Attempted: {len(successes) + len(failures)}", log_file)


# -----------------
# Main entry point
# -----------------


def main() -> None:
    global_start = time.time()
    args = parse_args()
    config = load_config(env_file=args.env_file)
    log_file = (
        config.logs_dir / f"restart_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    log(f"Env file: {args.env_file}", log_file)

    clusters = fetch_active_clusters(config)
    if not clusters:
        log_warning("No active compute clusters found. Nothing to do.", log_file)
        return

    api = ComputeApiClient(config=config, log_file=log_file)

    try:
        log("=" * 80, log_file)
        log("Compute Restart Tool", log_file)
        log(f"Environment: {config.api_base_url}", log_file)
        log(f"Mode: {'DRY RUN' if config.dry_run else 'EXECUTION'}", log_file)
        log(f"API Path Version: {COMPUTE_API_PATH_VERSION}", log_file)
        log(f"Total Targets: {len(clusters)}", log_file)
        log(f"Started At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", log_file)
        log("=" * 80, log_file)

        print_plan(clusters, api)

        if config.dry_run:
            log_warning("Dry run enabled. No API requests were sent.", log_file)
            return

        confirm = input("Type 'YES' to restart ALL active compute clusters: ").strip()
        if confirm.upper() != "YES":
            log("Execution aborted by user.", log_file)
            return

        execution_start = time.time()
        successes, failures = run_restart_pass(api, clusters, pass_name="PASS 1")

        # One workflow-level retry is useful for transient cluster-side issues.
        # This is different from request_with_retry(), which only retries HTTP calls.
        if failures:
            retry_clusters = [cluster for cluster, _ in failures]
            log_warning(
                f"Retrying {len(retry_clusters)} failed cluster(s) one more time before final export...",
                log_file,
            )
            retry_successes, retry_failures = run_restart_pass(
                api, retry_clusters, pass_name="PASS 2"
            )
            successes.extend(retry_successes)
            failures = retry_failures

        if failures:
            failed_file = write_failures_file(failures, config.logs_dir)
            log_error(f"Failed clusters were written to: {failed_file}", log_file)

        print_summary(
            clusters=clusters,
            successes=successes,
            failures=failures,
            log_file=log_file,
            global_start=global_start,
            execution_start=execution_start,
        )
    finally:
        api.close()


if __name__ == "__main__":
    main()
