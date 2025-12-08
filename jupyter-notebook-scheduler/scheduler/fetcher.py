import logging
import os
import tempfile
import shutil
import git
import s3fs

logger = logging.getLogger(__name__)


class InputFetcher:
    def __init__(self, config):
        self.config = config
        self.temp_dir = tempfile.mkdtemp(prefix="notebook_job_")

    def fetch(self):
        """
        Fetches the input (notebooks and dependencies) to a local temporary directory.
        Returns the path to the directory.
        """
        input_type = self.config.input_type
        
        if input_type == "git":
            return self._fetch_git()
        elif input_type == "s3":
            return self._fetch_s3()
        else:
            raise ValueError(f"Unknown input type: {input_type}")

    def _fetch_git(self):
        repo_url = self.config.input_path
        branch = self.config.git_branch
        token = self.config.git_token

        if token:
            # Insert token into URL for authentication
            # Assumes https://github.com/... format
            if repo_url.startswith("https://"):
                repo_url = repo_url.replace("https://", f"https://{token}@")
            else:
                logger.warning("Git token provided but URL does not start with https://. Token might not be used.")

        logger.info(f"Cloning git repo: {self.config.input_path} (branch: {branch})")
        
        try:
            git.Repo.clone_from(repo_url, self.temp_dir, branch=branch)
            logger.info("Git clone successful")
        except git.GitCommandError as e:
            logger.error(f"Git clone failed: {e}")
            raise

        return self.temp_dir

    def _fetch_s3(self):
        s3_path = self.config.input_path
        logger.info(f"Downloading from S3: {s3_path}")
        
        fs = s3fs.S3FileSystem()
        try:
            # Download recursively to temp dir
            # s3fs.get expects (remote, local)
            # If s3_path is a directory, we need to ensure we download contents
            fs.get(s3_path, self.temp_dir, recursive=True)
            logger.info("S3 download successful")
        except Exception as e:
            logger.error(f"S3 download failed: {e}")
            raise

        return self.temp_dir
