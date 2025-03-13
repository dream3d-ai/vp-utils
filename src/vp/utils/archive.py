import fnmatch
import json
import os
import zipfile
from datetime import datetime, timedelta, timezone


class WorkingDirectoryArchiver:
    """
    A class to handle archiving of working directories for jobs.

    This class creates a zip archive of the specified working directory, including job metadata
    and excluding files specified in a .gitignore file.

    Attributes:
        job_id (str): The ID of the job.
        output_dir (str): The directory where the archive will be saved.
    """

    def __init__(self, job_id: str, local_cache_dir: str, **metadata):
        """
        Initialize the WorkingDirectoryArchiver with job ID and job name.

        Args:
            job_id (str): The ID of the job.

        """
        self.job_id = job_id
        self.metadata = metadata

        self.local_cache_dir = os.path.join(local_cache_dir, job_id)
        os.makedirs(self.local_cache_dir, exist_ok=True)

    def archive(self, working_dir: str) -> str:
        """
        Create a zip archive of the specified working directory.

        This method reads the .gitignore file in the working directory to determine which files
        to exclude from the archive. It also includes job metadata in the archive.

        Args:
            working_dir (str): The path to the working directory to be archived.

        Returns:
            str: The path to the created zip archive.
        """
        archive_name = f"{os.path.basename(working_dir)}.zip"
        archive_path = os.path.join(self.local_cache_dir, archive_name)

        gitignore_path = os.path.join(working_dir, ".gitignore")
        ignore_patterns = []
        if os.path.exists(gitignore_path):
            with open(gitignore_path, "r") as gitignore_file:
                ignore_patterns = [
                    line.strip()
                    for line in gitignore_file
                    if line.strip() and not line.startswith("#")
                ]

        def should_ignore(path):
            """
            Determine if a file should be ignored based on .gitignore patterns.

            Args:
                path (str): The path to the file.

            Returns:
                bool: True if the file should be ignored, False otherwise.
            """
            rel_path = os.path.relpath(path, working_dir)
            return any(
                rel_path.startswith(pattern) or fnmatch.fnmatch(rel_path, pattern)
                for pattern in ignore_patterns
            )

        with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            metadata = {
                "creation_timestamp": str(
                    datetime.now().astimezone(timezone(timedelta(hours=-5), "EST"))
                ),
                **self.metadata,
            }
            zipf.writestr("job.json", json.dumps(metadata))

            # Archive files
            for root, dirs, files in os.walk(working_dir):
                dirs[:] = [
                    d
                    for d in dirs
                    if d != "__pycache__" and not should_ignore(os.path.join(root, d))
                ]
                for file in files:
                    file_path = os.path.join(root, file)
                    if not should_ignore(file_path):
                        arcname = os.path.relpath(file_path, working_dir)
                        zipf.write(file_path, arcname)

        return archive_path
