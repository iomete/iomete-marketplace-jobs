import logging
import sys
import os
import tempfile
import s3fs
from config import AppConfig
from executor import NotebookExecutor

logger = logging.getLogger(__name__)


def main():
    config = AppConfig.from_yaml()
    _configure_logging(config.job_context)
    logger.info("Starting Jupyter Notebook Scheduler Job")

    config.validate()

    storage = StorageHandler(config)
    output_path = None
    job_error = None

    try:
        # 1. Fetch input
        logger.info(f"Fetching input from {config.input_type}...")
        fetcher = InputFetcher(config)
        working_dir = fetcher.fetch()
        logger.info(f"Input fetched to {working_dir}")

        # 2. Execute notebook
        logger.info(f"Preparing execution for: {config.main_notebook_file}")
        executor = NotebookExecutor(config, working_dir)
        output_path = executor.get_output_path()
        executor.execute()
        logger.info(f"Notebook executed successfully. Output at: {output_path}")
    except Exception as e:
        job_error = e
        logger.error(f"Job failed: {e}", exc_info=True)
    finally:
        # Always upload whatever output exists: the full notebook on success,
        # the partially-executed one on failure (useful for debugging).
        if output_path and os.path.exists(output_path):
            try:
                logger.info(f"Uploading output to {config.output_s3_path}")
                s3_location = storage.upload(output_path)
                logger.info(f"Upload complete: {s3_location}")
            except Exception as upload_error:
                logger.error(f"Failed to upload output: {upload_error}", exc_info=True)
                job_error = job_error or upload_error

    if job_error is not None:
        sys.exit(1)


def _configure_logging(job_context):
    """Sets up logging so every line carries the job/run id for correlation."""
    logging.basicConfig(
        level=logging.INFO,
        format=(
            f"%(asctime)s - [job={job_context.job_id} run={job_context.run_id}] "
            "- %(name)s - %(levelname)s - %(message)s"
        ),
        handlers=[logging.StreamHandler(sys.stdout)],
    )


class StorageHandler:
    def __init__(self, config):
        self.config = config

    def upload(self, local_file_path):
        """
        Uploads the local file to the configured S3 destination.
        Returns the S3 URI of the uploaded file.
        """
        s3_base = self.config.output_s3_path.rstrip("/")

        run_id = self.config.job_context.run_id

        filename = os.path.basename(local_file_path)
        stem, ext = os.path.splitext(filename)

        # Construct destination path
        # Pattern: {s3_base}/runs/{filename}.{runId}.ipynb
        destination_path = f"{s3_base}/runs/{stem}.{run_id}{ext}"

        logger.info(f"Uploading {local_file_path} to {destination_path}")

        fs = s3fs.S3FileSystem()
        try:
            fs.put(local_file_path, destination_path)
            logger.info("Upload successful")
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise

        return destination_path


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

        if input_type == "s3":
            return self._fetch_s3()
        elif input_type in ("manual", "local"):
            return self._fetch_local()
        else:
            raise ValueError(f"Unknown input type: {input_type}")

    def _fetch_local(self):
        """Uses a locally-mounted path for manual injection.

        If a path is provided it must exist and is returned as-is; otherwise the
        (empty) temp dir is returned for the caller to populate.
        """
        local_path = self.config.input_path
        if not local_path:
            logger.info(f"No local path provided; using temp dir {self.temp_dir}")
            return self.temp_dir

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local input path not found: {local_path}")

        logger.info(f"Using local input path: {local_path}")
        return local_path

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


if __name__ == "__main__":
    main()
