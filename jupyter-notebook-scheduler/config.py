import os
import re
import yaml
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Matches ${VAR} and ${VAR:-default} style references inside string values.
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}^{:]+)(?::-([^}]*))?\}")


def _load_yaml(config_path):
    if not os.path.exists(config_path):
        logger.warning(f"Config file not found at {config_path}. Using empty config.")
        return {}

    with open(config_path, "r") as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            raise


def _expand_env_vars(value):
    """Recursively expand ${VAR} / ${VAR:-default} references in string values.

    This keeps secrets (e.g. the gateway token) out of the committed config
    file: they can be injected at runtime via environment variables.
    """
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    if isinstance(value, str):
        return _expand_string(value)
    return value


def _expand_string(value):
    def replace(match):
        var_name, default = match.group(1), match.group(2)
        resolved = os.environ.get(var_name)
        if resolved is None:
            if default is None:
                logger.warning(f"Environment variable '{var_name}' is not set and has no default.")
                return ""
            return default
        return resolved

    return _ENV_VAR_PATTERN.sub(replace, value)


@dataclass(frozen=True)
class JobContext:
    """Identifiers for the current IOMETE job run.

    Used to build the output S3 path and to correlate every log line back to a
    specific run. Sourced from the ``IOMETE_JOB_ID`` / ``IOMETE_JOB_RUN_ID``
    environment variables the platform injects into the job pod.
    """

    job_id: str
    run_id: str

    @classmethod
    def from_env(cls):
        job_id = os.environ.get("IOMETE_JOB_ID")
        run_id = os.environ.get("IOMETE_JOB_RUN_ID")

        if (job_id is None or run_id is None) and cls._in_cluster():
            # In-cluster these are always injected; missing values would silently
            # produce a wrong S3 path, so fail loudly instead.
            raise ValueError(
                "IOMETE_JOB_ID and IOMETE_JOB_RUN_ID must be set when running in-cluster "
                f"(got job_id={job_id!r}, run_id={run_id!r})."
            )

        if job_id is None or run_id is None:
            logger.warning(
                "IOMETE_JOB_ID / IOMETE_JOB_RUN_ID not set; using placeholder ids. "
                "Output path and logs will not be correlatable to a job run."
            )

        return cls(job_id=job_id or "unknown-job", run_id=run_id or "unknown-run")

    @staticmethod
    def _in_cluster():
        return "KUBERNETES_SERVICE_HOST" in os.environ


@dataclass(frozen=True)
class AppConfig:
    input_type: str = None
    input_path: str = None
    git_branch: str = "main"
    git_token: str = None
    main_notebook_file: str = None
    notebook_params: dict = field(default_factory=dict)
    gateway_url: str = None
    gateway_token: str = None
    gateway_kernel_name: str = "python3"
    gateway_auth_scheme: str = "token"
    gateway_request_timeout: int = 120
    gateway_connect_timeout: int = 60
    gateway_execution_timeout: int = 600
    output_s3_path: str = None
    job_context: JobContext = None

    @classmethod
    def from_yaml(cls, config_path=None):
        """Builds an AppConfig from a YAML file with env-var interpolation."""
        config_path = config_path or os.getenv("CONFIG_PATH", "/etc/config/config.yaml")
        data = _expand_env_vars(_load_yaml(config_path))

        input_cfg = data.get("input", {}) or {}
        notebook_cfg = data.get("notebook", {}) or {}
        gateway_cfg = data.get("gateway", {}) or {}
        output_cfg = data.get("output", {}) or {}

        return cls(
            input_type=input_cfg.get("type"),
            input_path=input_cfg.get("path"),
            git_branch=input_cfg.get("branch", "main"),
            git_token=input_cfg.get("token"),
            main_notebook_file=notebook_cfg.get("main_file"),
            notebook_params=notebook_cfg.get("parameters") or {},
            gateway_url=gateway_cfg.get("url"),
            gateway_token=gateway_cfg.get("token"),
            gateway_kernel_name=gateway_cfg.get("kernel_name", "python3"),
            gateway_auth_scheme=gateway_cfg.get("auth_scheme", "token"),
            gateway_request_timeout=gateway_cfg.get("request_timeout", 120),
            gateway_connect_timeout=gateway_cfg.get("connect_timeout", 60),
            gateway_execution_timeout=gateway_cfg.get("execution_timeout", 600),
            output_s3_path=output_cfg.get("s3_path"),
            job_context=JobContext.from_env(),
        )

    def validate(self):
        """Fail fast on missing required configuration."""
        missing = []
        if not self.input_type:
            missing.append("input.type")
        if not self.main_notebook_file:
            missing.append("notebook.main_file")
        if not self.gateway_url:
            missing.append("gateway.url")
        if not self.gateway_token:
            missing.append("gateway.token")
        if not self.output_s3_path:
            missing.append("output.s3_path")
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
