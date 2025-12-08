import logging
import sys
import os
from config import config
from scheduler.fetcher import InputFetcher
from scheduler.executor import NotebookExecutor
from scheduler.storage import StorageHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Jupyter Notebook Scheduler Job")
    
    output_path = None
    storage = StorageHandler(config)

    try:
        # 1. Fetch Input
        logger.info(f"Fetching input from {config.input_type}...")
        fetcher = InputFetcher(config)
        working_dir = fetcher.fetch()
        logger.info(f"Input fetched to {working_dir}")

        # 2. Prepare Executor
        logger.info(f"Preparing execution for: {config.main_notebook_file}")
        executor = NotebookExecutor(config, working_dir)
        output_path = executor.get_output_path()

        # 3. Execute Notebook
        try:
            executor.execute()
            logger.info(f"Notebook executed successfully. Output at: {output_path}")
        except Exception as exec_error:
            logger.error(f"Notebook execution failed: {exec_error}")
            # We continue to upload the failed notebook
            if output_path and os.path.exists(output_path):
                logger.info("Uploading failed notebook for debugging...")
                try:
                    s3_location = storage.upload(output_path)
                    logger.info(f"Failed notebook uploaded to: {s3_location}")
                except Exception as upload_error:
                    logger.error(f"Failed to upload failed notebook: {upload_error}")
            raise exec_error

        # 4. Upload Output (Success case)
        logger.info(f"Uploading output to {config.output_s3_path}")
        s3_location = storage.upload(output_path)
        logger.info(f"Upload complete: {s3_location}")

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
