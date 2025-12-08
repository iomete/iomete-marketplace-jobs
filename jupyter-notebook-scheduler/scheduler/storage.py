import logging
import os
import s3fs

logger = logging.getLogger(__name__)


class StorageHandler:
    def __init__(self, config):
        self.config = config

    def upload(self, local_file_path):
        """
        Uploads the local file to the configured S3 destination.
        Returns the S3 URI of the uploaded file.
        """
        s3_base = self.config.output_s3_path.rstrip("/")
        
        pod_name = os.environ.get("HOSTNAME", "unknown-pod")
        
        filename = os.path.basename(local_file_path)
        
        # Construct destination path
        # Pattern: {s3_base}/{pod_name}/{filename}
        destination_path = f"{s3_base}/{pod_name}/{filename}"
        
        logger.info(f"Uploading {local_file_path} to {destination_path}")
        
        fs = s3fs.S3FileSystem()
        try:
            fs.put(local_file_path, destination_path)
            logger.info("Upload successful")
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise
        
        return destination_path
