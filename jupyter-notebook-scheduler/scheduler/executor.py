import logging
import os
import papermill as pm

logger = logging.getLogger(__name__)


class NotebookExecutor:
    def __init__(self, config, working_dir):
        self.config = config
        self.working_dir = working_dir

    def get_output_path(self):
        """
        Returns the expected output path for the notebook.
        """
        input_path = os.path.join(self.working_dir, self.config.main_notebook_file)
        notebook_dir = os.path.dirname(input_path)
        notebook_name = os.path.basename(input_path)
        output_filename = f"output_{notebook_name}"
        return os.path.join(notebook_dir, output_filename)

    def execute(self):
        """
        Executes the notebook using Papermill.
        Returns the path to the executed notebook file.
        """
        input_path = os.path.join(self.working_dir, self.config.main_notebook_file)
        
        # Ensure input file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Notebook file not found: {input_path}")

        # Output file will be in the same directory as input, but with prefix
        # This ensures relative paths for assets (images etc) might still work if saved there
        notebook_dir = os.path.dirname(input_path)
        notebook_name = os.path.basename(input_path)
        output_filename = f"output_{notebook_name}"
        output_path = os.path.join(notebook_dir, output_filename)

        logger.info(f"Running notebook {input_path} -> {output_path}")
        logger.info(f"Parameters: {self.config.notebook_params}")
        
        try:
            pm.execute_notebook(
               input_path,
               output_path,
               parameters=self.config.notebook_params,
               cwd=self.working_dir,
               log_output=True, # Log to stdout/stderr
               report_mode=False # Don't hide code
            )
            logger.info("Notebook execution completed successfully")
        except pm.PapermillExecutionError as e:
            logger.error(f"Notebook execution failed: {e}")
            # We still return output_path because papermill saves the partial execution
            # which is useful for debugging.
            # However, we should probably re-raise or let main handle it.
            # Main expects success if it continues. 
            # But we want to upload the failed notebook too!
            # So we might want to catch it here, log it, and still return the path, 
            # but maybe signal failure?
            # For now, let's re-raise, but main.py should be smart enough to upload 
            # the output file even on failure if it exists.
            raise

        return output_path
