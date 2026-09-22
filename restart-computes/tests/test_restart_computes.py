import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import restart_computes as rc

ENV = {
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "iomete_core_db",
    "DB_USER": "postgres",
    "DB_PASSWORD": "secret",
    "API_BASE_URL": "https://release.iomete.cloud",
    "API_TOKEN": "token",
    "DRY_RUN": "false",
}


def cluster(name="alpha", compute_id="id-alpha", domain="default", namespace="ns-1"):
    return rc.Cluster(
        compute_id=compute_id,
        domain=domain,
        namespace=namespace,
        name=name,
        driver_status="ACTIVE",
    )


class FakeApi:
    instances: list["FakeApi"] = []
    setup_hook = None

    def __init__(self, config, log_file):
        self.config = config
        self.log_file = log_file
        self.calls: list[tuple[str, str]] = []
        self.status: dict[str, str] = {}
        self.fail_on: dict[str, set[str]] = {}
        self.fail_once: dict[str, set[str]] = {}
        self.closed = False
        FakeApi.instances.append(self)
        if FakeApi.setup_hook is not None:
            FakeApi.setup_hook(self)

    def close(self):
        self.closed = True

    def stop_url(self, c):
        return f"{self.config.api_base_url}/stop/{c.compute_id}"

    def start_url(self, c):
        return f"{self.config.api_base_url}/start/{c.compute_id}"

    def get_compute_details(self, c):
        return {"driverStatus": self.status.get(c.compute_id, "ACTIVE")}

    def _act(self, verb, c, lands_on):
        self.calls.append((verb, c.compute_id))
        if verb in self.fail_once.get(c.compute_id, set()):
            self.fail_once[c.compute_id].discard(verb)
            raise RuntimeError(f"{verb.upper()} failed once for {c.compute_id}")
        if verb in self.fail_on.get(c.compute_id, set()):
            raise RuntimeError(f"{verb.upper()} failed for {c.compute_id}")
        self.status[c.compute_id] = lands_on

    def stop_compute(self, c):
        self._act("stop", c, "STOPPED")

    def start_compute(self, c):
        self._act("start", c, "ACTIVE")


@pytest.fixture(autouse=True)
def isolated_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(rc, "ComputeApiClient", FakeApi)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "YES")
    FakeApi.instances = []
    FakeApi.setup_hook = None
    yield tmp_path
    FakeApi.setup_hook = None


def run_main(monkeypatch, argv, discovered=None, setup=None):
    monkeypatch.setattr(
        sys, "argv", ["restart_computes.py", "--env-file", "missing.env", *argv]
    )
    if discovered is not None:

        def _discover(_config, domain=None):
            rows = list(discovered)
            return [c for c in rows if c.domain == domain] if domain else rows

        monkeypatch.setattr(rc, "fetch_active_clusters", _discover)
    else:

        def _no_db(_config, domain=None):
            raise AssertionError("START must not query PostgreSQL")

        monkeypatch.setattr(rc, "fetch_active_clusters", _no_db)

    FakeApi.setup_hook = setup
    return rc.main()


def state_files(tmp_path):
    return sorted((tmp_path / "logs").glob("stopped_computes_*.json"))


def read_state(tmp_path):
    return json.loads(state_files(tmp_path)[0].read_text())


def written_artifact(tmp_path, clusters):
    config = rc.load_config(env_file="missing.env")
    return rc.write_stopped_artifact(clusters, config, tmp_path)


# --- STOP and the state file ----------------------------------------------


def test_stop_writes_only_successfully_stopped_computes(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--mode", "stop"],
        discovered=[cluster("alpha", "id-a"), cluster("beta", "id-b")],
        setup=lambda api: api.fail_on.update({"id-b": {"stop"}}),
    )

    assert [c["id"] for c in read_state(tmp_path)["computes"]] == ["id-a"]
    assert code == rc.EXIT_FAILURES
    assert list((tmp_path / "logs").glob("failed_clusters_*.txt"))


def test_artifact_schema_fields_and_null_namespace(tmp_path, monkeypatch):
    run_main(
        monkeypatch,
        ["--mode", "stop"],
        discovered=[cluster("alpha", "id-a", namespace=None)],
    )

    payload = read_state(tmp_path)
    assert set(payload) == {"stopped_at", "api_base_url", "computes"}
    assert payload["api_base_url"] == ENV["API_BASE_URL"]
    assert payload["computes"] == [
        {"id": "id-a", "domain": "default", "name": "alpha", "namespace": None}
    ]


def test_load_stopped_artifact_round_trip(tmp_path):
    config = rc.load_config(env_file="missing.env")
    source = [cluster("alpha", "id-a", namespace=None), cluster("beta", "id-b")]

    path = rc.write_stopped_artifact(source, config, tmp_path)
    loaded, stopped_at = rc.load_stopped_artifact(path, config)

    assert [(c.compute_id, c.domain, c.name, c.namespace) for c in loaded] == [
        ("id-a", "default", "alpha", None),
        ("id-b", "default", "beta", "ns-1"),
    ]
    assert all(c.driver_status == "STOPPED" for c in loaded)
    assert stopped_at.startswith("20")


def test_stop_writes_no_artifact_when_everything_fails(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--mode", "stop"],
        discovered=[cluster("alpha", "id-a")],
        setup=lambda api: api.fail_on.update({"id-a": {"stop"}}),
    )

    assert state_files(tmp_path) == []
    assert code == rc.EXIT_FAILURES


def test_repeated_stop_writes_a_second_artifact(tmp_path, monkeypatch):
    run_main(monkeypatch, ["--mode", "stop"], discovered=[cluster("alpha", "id-a")])
    run_main(monkeypatch, ["--mode", "stop"], discovered=[cluster("beta", "id-b")])

    payloads = [json.loads(p.read_text()) for p in state_files(tmp_path)]
    assert len(payloads) == 2
    assert [c["id"] for p in payloads for c in p["computes"]] == ["id-a", "id-b"]


# --- START -----------------------------------------------------------------


def test_start_uses_the_artifact_and_never_queries_postgres(tmp_path, monkeypatch):
    path = written_artifact(tmp_path, [cluster("alpha", "id-a")])

    code = run_main(
        monkeypatch,
        ["--mode", "start", "--state-file", str(path)],
        setup=lambda api: api.status.update({"id-a": "STOPPED"}),
    )

    assert FakeApi.instances[-1].calls == [("start", "id-a")]
    assert code == rc.EXIT_OK


def test_start_does_not_require_database_config(tmp_path, monkeypatch):
    path = written_artifact(tmp_path, [cluster("alpha", "id-a")])
    for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        monkeypatch.delenv(key)

    code = run_main(
        monkeypatch,
        ["--mode", "start", "--state-file", str(path)],
        setup=lambda api: api.status.update({"id-a": "STOPPED"}),
    )

    assert code == rc.EXIT_OK


@pytest.mark.parametrize("current", ["ACTIVE", "STARTING"])
def test_start_skips_a_compute_that_is_already_running(tmp_path, monkeypatch, current):
    path = written_artifact(tmp_path, [cluster("alpha", "id-a")])

    code = run_main(
        monkeypatch,
        ["--mode", "start", "--state-file", str(path)],
        setup=lambda api: api.status.update({"id-a": current}),
    )

    assert FakeApi.instances[-1].calls == []
    assert code == rc.EXIT_OK
    assert path.exists()


def test_rerunning_start_is_safe(tmp_path, monkeypatch):
    path = written_artifact(tmp_path, [cluster("alpha", "id-a"), cluster("beta", "id-b")])

    first = run_main(
        monkeypatch,
        ["--mode", "start", "--state-file", str(path)],
        setup=lambda api: (
            api.status.update({"id-a": "STOPPED", "id-b": "STOPPED"}),
            api.fail_on.update({"id-b": {"start"}}),
        ),
    )
    second = run_main(
        monkeypatch,
        ["--mode", "start", "--state-file", str(path)],
        setup=lambda api: api.status.update({"id-a": "ACTIVE", "id-b": "STOPPED"}),
    )

    assert first == rc.EXIT_FAILURES
    # Only the compute that failed the first time is touched on the rerun.
    assert FakeApi.instances[-1].calls == [("start", "id-b")]
    assert second == rc.EXIT_OK
    assert path.exists()


def test_start_refuses_an_artifact_from_another_environment(tmp_path, monkeypatch):
    path = written_artifact(tmp_path, [cluster("alpha", "id-a")])
    payload = json.loads(path.read_text())
    payload["api_base_url"] = "https://other.iomete.cloud"
    path.write_text(json.dumps(payload))

    code = run_main(monkeypatch, ["--mode", "start", "--state-file", str(path)])

    assert code == rc.EXIT_CONFIG_ERROR
    assert FakeApi.instances == []


def test_start_reports_a_missing_artifact_as_a_config_error(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch, ["--mode", "start", "--state-file", str(tmp_path / "gone.json")]
    )

    assert code == rc.EXIT_CONFIG_ERROR
    assert FakeApi.instances == []


# --- RESTART ---------------------------------------------------------------


def test_restart_stops_then_starts_and_writes_no_artifact(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch, ["--mode", "restart"], discovered=[cluster("alpha", "id-a")]
    )

    assert FakeApi.instances[-1].calls == [("stop", "id-a"), ("start", "id-a")]
    assert state_files(tmp_path) == []
    assert code == rc.EXIT_OK


def test_restart_is_the_default_mode(tmp_path, monkeypatch):
    run_main(monkeypatch, [], discovered=[cluster("alpha", "id-a")])

    assert FakeApi.instances[-1].calls == [("stop", "id-a"), ("start", "id-a")]


def test_restart_failure_exits_non_zero(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--mode", "restart"],
        discovered=[cluster("alpha", "id-a")],
        setup=lambda api: api.fail_on.update({"id-a": {"start"}}),
    )

    assert code == rc.EXIT_FAILURES


# --- CLI, dry run, retry ---------------------------------------------------


def test_start_without_state_file_is_a_usage_error(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["restart_computes.py", "--mode", "start"])
    with pytest.raises(SystemExit) as excinfo:
        rc.parse_args()
    assert excinfo.value.code == rc.EXIT_CONFIG_ERROR


def test_state_file_is_rejected_outside_start_mode(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["restart_computes.py", "--mode", "stop", "--state-file", "x.json"]
    )
    with pytest.raises(SystemExit) as excinfo:
        rc.parse_args()
    assert excinfo.value.code == rc.EXIT_CONFIG_ERROR


def test_dry_run_sends_nothing_and_writes_no_artifact(tmp_path, monkeypatch):
    monkeypatch.setenv("DRY_RUN", "true")

    code = run_main(
        monkeypatch, ["--mode", "stop"], discovered=[cluster("alpha", "id-a")]
    )

    assert FakeApi.instances[-1].calls == []
    assert state_files(tmp_path) == []
    assert code == rc.EXIT_OK


def test_abort_at_the_prompt_sends_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _prompt="": "no")

    code = run_main(
        monkeypatch, ["--mode", "stop"], discovered=[cluster("alpha", "id-a")]
    )

    assert FakeApi.instances[-1].calls == []
    assert state_files(tmp_path) == []
    assert code == rc.EXIT_OK


def test_retry_pass_recovers_a_transient_failure(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--mode", "stop"],
        discovered=[cluster("alpha", "id-a")],
        setup=lambda api: api.fail_once.update({"id-a": {"stop"}}),
    )

    assert [c["id"] for c in read_state(tmp_path)["computes"]] == ["id-a"]
    assert code == rc.EXIT_OK


# --- domain filter ---------------------------------------------------------


class RecordingCursor:
    def __init__(self, sink):
        self.sink = sink

    def execute(self, query, params=None):
        self.sink.append((query, params))

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class RecordingConnection:
    def __init__(self, sink):
        self.sink = sink

    def cursor(self):
        return RecordingCursor(self.sink)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def capture_discovery_query(monkeypatch, domain):
    sink = []
    monkeypatch.setattr(rc, "open_db_connection", lambda _config: RecordingConnection(sink))
    rc.fetch_active_clusters(None, domain)
    return sink[0]


def test_domain_filter_is_a_parameterized_query(monkeypatch):
    query, params = capture_discovery_query(monkeypatch, "fde")

    assert "AND domain = %s" in query
    assert params == ("fde",)
    assert "fde" not in query


def test_discovery_without_domain_is_unchanged(monkeypatch):
    query, params = capture_discovery_query(monkeypatch, None)

    assert "domain = " not in query
    assert "driver_status = 'ACTIVE'" in query
    assert params is None


def test_restart_with_domain_targets_only_that_domain(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--domain", "fde"],
        discovered=[
            cluster("alpha", "id-a", domain="fde"),
            cluster("beta", "id-b", domain="default"),
        ],
    )

    assert FakeApi.instances[-1].calls == [("stop", "id-a"), ("start", "id-a")]
    assert code == rc.EXIT_OK


def test_restart_with_domain_works_when_mode_is_explicit(tmp_path, monkeypatch):
    run_main(
        monkeypatch,
        ["--mode", "restart", "--domain", "fde"],
        discovered=[
            cluster("alpha", "id-a", domain="fde"),
            cluster("beta", "id-b", domain="default"),
        ],
    )

    assert FakeApi.instances[-1].calls == [("stop", "id-a"), ("start", "id-a")]


def test_restart_without_domain_targets_every_active_compute(tmp_path, monkeypatch):
    code = run_main(
        monkeypatch,
        ["--mode", "restart"],
        discovered=[
            cluster("alpha", "id-a", domain="fde"),
            cluster("beta", "id-b", domain="default"),
        ],
    )

    assert FakeApi.instances[-1].calls == [
        ("stop", "id-a"),
        ("start", "id-a"),
        ("stop", "id-b"),
        ("start", "id-b"),
    ]
    assert code == rc.EXIT_OK


def test_domain_with_no_match_reports_the_domain(tmp_path, monkeypatch, capsys):
    code = run_main(
        monkeypatch,
        ["--domain", "missing"],
        discovered=[cluster("alpha", "id-a", domain="fde")],
    )

    assert "No ACTIVE computes found in domain missing." in capsys.readouterr().out
    assert FakeApi.instances == []
    assert code == rc.EXIT_OK


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_domain_is_rejected(monkeypatch, value):
    monkeypatch.setattr(sys, "argv", ["restart_computes.py", "--domain", value])

    with pytest.raises(SystemExit) as excinfo:
        rc.parse_args()

    assert excinfo.value.code == rc.EXIT_CONFIG_ERROR


@pytest.mark.parametrize("mode", ["stop", "start"])
def test_domain_is_rejected_outside_restart_mode(monkeypatch, mode):
    argv = ["restart_computes.py", "--mode", mode, "--domain", "fde"]
    if mode == "start":
        argv += ["--state-file", "x.json"]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(SystemExit) as excinfo:
        rc.parse_args()

    assert excinfo.value.code == rc.EXIT_CONFIG_ERROR
