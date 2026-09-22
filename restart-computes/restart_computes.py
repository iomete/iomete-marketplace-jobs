import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection
from requests import Response, Session
from requests.exceptions import RequestException

RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

# This is the backend compute API path version used by this script.
COMPUTE_API_PATH_VERSION = "v2"

MODE_STOP = "stop"
MODE_START = "start"
MODE_RESTART = "restart"

OUTCOME_DONE = "done"
OUTCOME_SKIPPED = "skipped"

# Starting a compute that is already up re-applies its SparkApplication, so START skips these.
ALREADY_RUNNING_STATUSES = frozenset({"ACTIVE", "STARTING"})

EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFIG_ERROR = 2

RULE_WIDTH = 78


@dataclass(frozen=True)
class Cluster:
    compute_id: str
    domain: str
    namespace: str | None
    name: str
    driver_status: str


@dataclass(frozen=True)
class PollConfig:
    base_interval_seconds: float
    max_interval_seconds: float
    timeout_seconds: float
    backoff_multiplier: float


@dataclass(frozen=True)
class Config:
    db_host: str | None
    db_port: str | None
    db_name: str | None
    db_user: str | None
    db_password: str | None
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


def load_config(env_file: str = ".env", require_db: bool = True) -> Config:
    load_dotenv(dotenv_path=env_file, override=True)

    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    # START uses the state file and does not need database access.
    read_db = get_env if require_db else os.getenv

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
        db_host=read_db("DB_HOST"),
        db_port=read_db("DB_PORT"),
        db_name=read_db("DB_NAME"),
        db_user=read_db("DB_USER"),
        db_password=read_db("DB_PASSWORD"),
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
        description="Stop, start, or restart compute clusters in an IOMETE environment."
    )
    parser.add_argument(
        "--mode",
        choices=[MODE_STOP, MODE_START, MODE_RESTART],
        default=MODE_RESTART,
        help=(
            "stop: stop active computes and write a state file. "
            "start: start the computes listed in a state file. "
            "restart: stop then start in one run. Defaults to restart."
        ),
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the environment file to load. Defaults to .env",
    )
    parser.add_argument(
        "--state-file",
        help="State file written by --mode stop. Required by --mode start.",
    )
    parser.add_argument(
        "--domain",
        help="Restart only the active computes in this domain. Restart mode only.",
    )
    args = parser.parse_args()

    if args.mode == MODE_START and not args.state_file:
        parser.error("--mode start requires --state-file")
    if args.mode != MODE_START and args.state_file:
        parser.error("--state-file is only used with --mode start")
    if args.domain is not None and not args.domain.strip():
        parser.error("--domain requires a non-empty value")
    if args.domain and args.mode != MODE_RESTART:
        parser.error("--domain is only used with --mode restart")

    return args


# --------
# Logging
# --------

LABEL_COLORS = {
    "OK": GREEN,
    "SKIP": CYAN,
    "FAIL": RED,
    "RETRY": YELLOW,
    "STOP": YELLOW,
    "START": CYAN,
}


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


def log_status(label: str, message: str, log_file: Path) -> None:
    # The label is text, not just color, so the log file reads the same as the terminal.
    _write_log(f"[{label:<5}] {message}", log_file, color=LABEL_COLORS.get(label))


def describe(cluster: Cluster) -> str:
    return (
        f"{cluster.name} ({cluster.compute_id}) "
        f"domain={cluster.domain} namespace={cluster.namespace or '-'}"
    )


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


def fetch_active_clusters(config: Config, domain: str | None = None) -> list[Cluster]:
    query = """
        SELECT id, domain, namespace, name, driver_status
        FROM lakehouse
        WHERE is_deleted = false
          AND driver_status = 'ACTIVE'
    """.rstrip()
    params: tuple[str, ...] | None = None

    if domain:
        query += "\n          AND domain = %s"
        params = (domain,)

    query += "\n        ORDER BY created_at DESC;"

    with open_db_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return [Cluster(*row) for row in rows]


# ---------------------
# Compute API operations
# ---------------------
class ComputeApiClient:

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
                    log_status(
                        "RETRY",
                        f"{method.upper()} {url} failed, attempt {attempt + 1}/{self.config.api_retry_count} "
                        f"in {self.config.api_retry_delay_seconds}s: {exc}",
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


def extract_driver_status(payload: dict[str, Any]) -> str | None:
    value = payload.get("driverStatus")
    return str(value) if value is not None else None


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
    started_at = time.time()
    sleep_seconds = poll.base_interval_seconds
    bad_states = {"FAILED", "ERROR"}
    reported_status: str | None = None
    last_progress_at = started_at

    while True:
        payload = api.get_compute_details(cluster)
        status = extract_driver_status(payload)
        elapsed = time.time() - started_at

        # Log transitions only. A slow start would otherwise emit a line every few seconds.
        if status != reported_status:
            log_status(
                phase_name,
                f"{cluster.name}: {status or 'unknown'} ({elapsed:.1f}s)",
                api.log_file,
            )
            reported_status = status
            last_progress_at = time.time()
        elif time.time() - last_progress_at >= 20:
            log_status(
                phase_name,
                f"{cluster.name}: {status or 'unknown'} ({elapsed:.1f}s, still waiting)",
                api.log_file,
            )
            last_progress_at = time.time()

        if status is None:
            return False, status

        if status in bad_states:
            error_message = extract_driver_error_message(payload)
            if error_message:
                log_error(
                    f"[{phase_name}] {cluster.name} driver error: {error_message}",
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


# -----------------
# Lifecycle actions
# -----------------


def stop_cluster(api: ComputeApiClient, cluster: Cluster) -> str:
    log_status("STOP", describe(cluster), api.log_file)
    api.stop_compute(cluster)

    reached, status = wait_for_cluster_state(
        api=api,
        cluster=cluster,
        target_status="STOPPED",
        poll=api.config.stop_poll,
        phase_name="STOP",
    )
    if not reached:
        raise RuntimeError(
            f"STOP did not reach STOPPED. Last observed status: {status or 'unknown'}"
        )

    return OUTCOME_DONE


def start_cluster(api: ComputeApiClient, cluster: Cluster) -> str:
    log_status("START", describe(cluster), api.log_file)

    current_status = extract_driver_status(api.get_compute_details(cluster))
    if current_status in ALREADY_RUNNING_STATUSES:
        log_status("SKIP", f"{cluster.name} is already {current_status}", api.log_file)
        return OUTCOME_SKIPPED

    api.start_compute(cluster)

    reached, status = wait_for_cluster_state(
        api=api,
        cluster=cluster,
        target_status="ACTIVE",
        poll=api.config.start_poll,
        phase_name="START",
    )
    if not reached:
        raise RuntimeError(
            f"START did not reach ACTIVE. Last observed status: {status or 'unknown'}"
        )

    return OUTCOME_DONE


def restart_cluster(api: ComputeApiClient, cluster: Cluster) -> str:
    stop_cluster(api, cluster)
    return start_cluster(api, cluster)


ACTIONS: dict[str, Callable[[ComputeApiClient, Cluster], str]] = {
    MODE_STOP: stop_cluster,
    MODE_START: start_cluster,
    MODE_RESTART: restart_cluster,
}


def run_pass(
    api: ComputeApiClient,
    clusters: list[Cluster],
    action: Callable[[ComputeApiClient, Cluster], str],
) -> tuple[list[Cluster], list[Cluster], list[tuple[Cluster, str]]]:
    successes: list[Cluster] = []
    skipped: list[Cluster] = []
    failures: list[tuple[Cluster, str]] = []

    for cluster in clusters:
        started_at = time.time()
        try:
            outcome = action(api, cluster)
        except Exception as exc:
            log_status("FAIL", f"{cluster.name}: {exc}", api.log_file)
            failures.append((cluster, str(exc)))
            continue

        if outcome == OUTCOME_SKIPPED:
            skipped.append(cluster)
        else:
            log_status(
                "OK", f"{cluster.name} in {time.time() - started_at:.1f}s", api.log_file
            )
            successes.append(cluster)

    return successes, skipped, failures


# --------------------------
# State file (STOP -> START)
# --------------------------


def write_stopped_artifact(
    clusters: list[Cluster], config: Config, logs_dir: Path
) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    state_file = logs_dir / f"stopped_computes_{stamp}.json"
    # Two stops in the same second would otherwise clobber the first file.
    suffix = 2
    while state_file.exists():
        state_file = logs_dir / f"stopped_computes_{stamp}_{suffix}.json"
        suffix += 1

    payload = {
        "stopped_at": datetime.now(timezone.utc).isoformat(),
        "api_base_url": config.api_base_url,
        "computes": [
            {
                "id": cluster.compute_id,
                "domain": cluster.domain,
                "name": cluster.name,
                "namespace": cluster.namespace,
            }
            for cluster in clusters
        ],
    }

    with state_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    return state_file


def load_stopped_artifact(
    state_file: Path, config: Config
) -> tuple[list[Cluster], str]:
    with state_file.open(encoding="utf-8") as f:
        payload = json.load(f)

    artifact_base_url = str(payload.get("api_base_url") or "").rstrip("/")
    if artifact_base_url != config.api_base_url:
        raise RuntimeError(
            f"State file was written for {artifact_base_url or '<missing>'} "
            f"but this run is configured for {config.api_base_url}."
        )

    clusters = [
        Cluster(
            compute_id=item["id"],
            domain=item["domain"],
            namespace=item.get("namespace"),
            name=item["name"],
            driver_status="STOPPED",
        )
        for item in payload.get("computes", [])
    ]

    return clusters, str(payload.get("stopped_at", "unknown"))


# --------------------
# Reporting / printing
# --------------------


def print_header(
    mode: str,
    config: Config,
    env_file: str,
    target_count: int,
    log_file: Path,
    source_note: str | None = None,
    domain: str | None = None,
) -> None:
    run_mode = "DRY RUN" if config.dry_run else "EXECUTION"

    log("=" * RULE_WIDTH, log_file)
    _write_log(
        f" Compute Tool  |  {mode.upper()}  |  {run_mode}", log_file, color=BOLD + CYAN
    )
    log("=" * RULE_WIDTH, log_file)
    log(f" Environment : {config.api_base_url}", log_file)
    log(f" Env file    : {env_file}", log_file)
    log(f" API version : {COMPUTE_API_PATH_VERSION}", log_file)
    if domain:
        log(f" Domain      : {domain}", log_file)
    log(f" Targets     : {target_count}", log_file)
    if source_note:
        log(f" Source      : {source_note}", log_file)
    log(f" Started     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", log_file)
    log(f" Log file    : {log_file}", log_file)
    log("=" * RULE_WIDTH, log_file)


def print_plan(clusters: list[Cluster], api: ComputeApiClient, mode: str) -> None:
    log(
        f" {'NAME':<28} {'DOMAIN':<16} {'NAMESPACE':<20} {'STATUS':<9} ID",
        api.log_file,
    )

    for cluster in clusters:
        log(
            f" {cluster.name:<28} {cluster.domain:<16} "
            f"{(cluster.namespace or '-'):<20} {cluster.driver_status:<9} "
            f"{cluster.compute_id}",
            api.log_file,
        )
        # URLs are worth the extra lines only when nothing is going to be sent.
        if api.config.dry_run:
            if mode in (MODE_STOP, MODE_RESTART):
                log(f"   STOP  -> {api.stop_url(cluster)}", api.log_file)
            if mode in (MODE_START, MODE_RESTART):
                log(f"   START -> {api.start_url(cluster)}", api.log_file)

    log("-" * RULE_WIDTH, api.log_file)


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
    mode: str,
    clusters: list[Cluster],
    successes: list[Cluster],
    skipped: list[Cluster],
    failures: list[tuple[Cluster, str]],
    log_file: Path,
    global_start: float,
    execution_start: float,
    state_file: Path | None,
    failed_file: Path | None,
    env_file: str,
    domain: str | None = None,
) -> None:
    log("=" * RULE_WIDTH, log_file)
    _write_log(f" {mode.upper()} SUMMARY", log_file, color=BOLD)
    log("=" * RULE_WIDTH, log_file)
    log(f" Succeeded : {len(successes)}", log_file)
    log(f" Skipped   : {len(skipped)}", log_file)
    log(f" Failed    : {len(failures)}", log_file)
    log(f" Targets   : {len(clusters)}", log_file)
    if domain:
        log(f" Domain    : {domain}", log_file)
    log(
        f" Duration  : {time.time() - execution_start:.1f}s "
        f"(total {time.time() - global_start:.1f}s)",
        log_file,
    )

    for cluster in skipped:
        log_status(
            "SKIP", f"{cluster.name} | {cluster.domain} | {cluster.compute_id}", log_file
        )

    for cluster, error in failures:
        log_status(
            "FAIL", f"{cluster.name} | {cluster.domain} | {cluster.compute_id}", log_file
        )
        log_error(f"        {error}", log_file)

    if failed_file:
        log_error(f" Failed computes written to: {failed_file}", log_file)

    if failures and mode == MODE_STOP:
        log_error(
            " Resolve these before the deployment. They may still be holding resources.",
            log_file,
        )

    if state_file is not None:
        log("-" * RULE_WIDTH, log_file)
        _write_log(f" STATE FILE  {state_file}", log_file, color=BOLD + GREEN)
        log(" Run this after the deployment:", log_file)
        _write_log(
            f"   python3 restart_computes.py --mode start --env-file {env_file} --state-file {state_file}",
            log_file,
            color=GREEN,
        )
        log("-" * RULE_WIDTH, log_file)


# -----------------
# Main entry point
# -----------------


def main() -> int:
    global_start = time.time()
    args = parse_args()

    try:
        config = load_config(
            env_file=args.env_file,
            require_db=args.mode != MODE_START,
        )
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_ERROR

    log_file = (
        config.logs_dir / f"restart_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    source_note: str | None = None
    try:
        if args.mode == MODE_START:
            clusters, stopped_at = load_stopped_artifact(Path(args.state_file), config)
            source_note = f"{args.state_file} (stopped at {stopped_at})"
        else:
            clusters = fetch_active_clusters(config, args.domain)
    except Exception as exc:
        log_status("FAIL", str(exc), log_file)
        return EXIT_CONFIG_ERROR

    print_header(
        args.mode,
        config,
        args.env_file,
        len(clusters),
        log_file,
        source_note,
        args.domain,
    )

    if not clusters:
        if args.domain:
            log_warning(f"No ACTIVE computes found in domain {args.domain}.", log_file)
        else:
            log_warning("Nothing to do.", log_file)
        return EXIT_OK

    api = ComputeApiClient(config=config, log_file=log_file)

    try:
        print_plan(clusters, api, args.mode)

        if config.dry_run:
            log_warning("Dry run enabled. No API requests were sent.", log_file)
            return EXIT_OK

        confirm = input(
            f"Type 'YES' to {args.mode} {len(clusters)} compute cluster(s): "
        ).strip()
        if confirm.upper() != "YES":
            log("Execution aborted by user.", log_file)
            return EXIT_OK

        execution_start = time.time()
        action = ACTIONS[args.mode]
        successes, skipped, failures = run_pass(api, clusters, action)

        # One workflow-level retry is useful for transient cluster-side issues.
        # This is different from request_with_retry(), which only retries HTTP calls.
        if failures:
            retry_clusters = [cluster for cluster, _ in failures]
            log_status(
                "RETRY",
                f"{len(retry_clusters)} compute(s): "
                + ", ".join(cluster.name for cluster in retry_clusters),
                log_file,
            )
            retry_successes, retry_skipped, failures = run_pass(
                api, retry_clusters, action
            )
            successes.extend(retry_successes)
            skipped.extend(retry_skipped)

        state_file = (
            write_stopped_artifact(successes, config, config.logs_dir)
            if args.mode == MODE_STOP and successes
            else None
        )
        failed_file = (
            write_failures_file(failures, config.logs_dir) if failures else None
        )

        print_summary(
            mode=args.mode,
            clusters=clusters,
            successes=successes,
            skipped=skipped,
            failures=failures,
            log_file=log_file,
            global_start=global_start,
            execution_start=execution_start,
            state_file=state_file,
            failed_file=failed_file,
            env_file=args.env_file,
            domain=args.domain,
        )

        return EXIT_FAILURES if failures else EXIT_OK
    finally:
        api.close()


if __name__ == "__main__":
    sys.exit(main())
