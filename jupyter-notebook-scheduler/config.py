import os
import yaml
import logging

logger = logging.getLogger(__name__)


class AppConfig:
    def __init__(self, config_path=None):
        self.config_path = config_path or os.getenv("CONFIG_PATH", "/etc/config/config.yaml")
        self.config = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found at {self.config_path}. Using empty config.")
            return {}
        
        with open(self.config_path, "r") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:
                logger.error(f"Error parsing config file: {e}")
                raise

    @property
    def input_type(self):
        return self.config.get("input", {}).get("type")

    @property
    def input_path(self):
        return self.config.get("input", {}).get("path")

    @property
    def git_branch(self):
        return self.config.get("input", {}).get("branch", "main")

    @property
    def git_token(self):
        return self.config.get("input", {}).get("token")

    @property
    def main_notebook_file(self):
        return self.config.get("notebook", {}).get("main_file")

    @property
    def notebook_params(self):
        return self.config.get("notebook", {}).get("parameters", {})

    @property
    def output_s3_path(self):
        return self.config.get("output", {}).get("s3_path")


# Global instance
config = AppConfig()
