#!/usr/bin/env python3
"""
Optimized File Handler

This module provides an optimized file handler that enforces strict 5MB file size limits
and reduces memory usage from excessive logging.
"""

import gzip
import os
import shutil
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


class OptimizedRotatingFileHandler(RotatingFileHandler):
    """
    Optimized rotating file handler with strict 5MB file size limit
    and automatic compression of old log files.
    """

    def __init__(self, filename, mode='a', maxBytes=5242880, backupCount=10, encoding=None, delay=False):
        """
        Initialize the optimized rotating file handler

        Args:
            filename: Log file path
            mode: File mode (default 'a' for append)
            maxBytes: Maximum file size in bytes (default 5MB)
            backupCount: Number of backup files to keep (default 10)
            encoding: File encoding (default None)
            delay: Whether to delay file opening (default False)
        """
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.max_bytes = maxBytes
        self.backup_count = backupCount
        self.log_dir = Path(filename).parent
        self.base_filename = Path(filename).stem

    def shouldRollover(self, record):
        """
        Determine if rollover should occur based on file size
        """
        if self.stream is None:
            self.stream = self._open()

        # Check if current file exceeds 5MB
        if self.stream.tell() >= self.max_bytes:
            return True

        return False

    def doRollover(self):
        """
        Perform rollover with compression and cleanup
        """
        if self.stream:
            self.stream.close()
            self.stream = None

        # Compress old backup files to save space
        self._compress_old_backups()

        # Remove excess backup files
        self._cleanup_old_backups()

        # Rename current file to backup
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = self.rotation_filename(f"{self.base_filename}.{i}")
                dfn = self.rotation_filename(f"{self.base_filename}.{i + 1}")
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)

            dfn = self.rotation_filename(f"{self.base_filename}.1")
            if os.path.exists(dfn):
                os.remove(dfn)
            os.rename(self.baseFilename, dfn)

        # Create new log file
        if not self.delay:
            self.stream = self._open()

    def _compress_old_backups(self):
        """
        Compress backup files older than 1 day to save space
        """
        try:
            for i in range(1, self.backupCount + 1):
                backup_file = self.rotation_filename(f"{self.base_filename}.{i}")
                if os.path.exists(backup_file):
                    # Check if file is older than 1 day
                    file_age = datetime.now().timestamp() - os.path.getmtime(backup_file)
                    if file_age > 86400:  # 24 hours in seconds
                        self._compress_file(backup_file)
        except Exception as e:
            # Log error but don't fail rollover
            print(f"Error compressing backup files: {e}")

    def _compress_file(self, file_path: str):
        """
        Compress a single file using gzip
        """
        try:
            compressed_path = f"{file_path}.gz"

            with open(file_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Remove original file after successful compression
            os.remove(file_path)

        except Exception as e:
            print(f"Error compressing {file_path}: {e}")

    def _cleanup_old_backups(self):
        """
        Remove backup files beyond the backup count limit
        """
        try:
            # Remove compressed files beyond backup count
            for i in range(self.backupCount + 1, self.backupCount + 5):
                compressed_file = f"{self.rotation_filename(f'{self.base_filename}.{i}')}.gz"
                if os.path.exists(compressed_file):
                    os.remove(compressed_file)
        except Exception as e:
            print(f"Error cleaning up old backups: {e}")

    def rotation_filename(self, name):
        """
        Generate rotation filename
        """
        return os.path.join(self.log_dir, name)


class SizeLimitedFileHandler(OptimizedRotatingFileHandler):
    """
    File handler with additional size monitoring and automatic cleanup
    """

    def __init__(self, filename, mode='a', maxBytes=5242880, backupCount=10, encoding=None, delay=False):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.total_logs_created = 0
        self.last_cleanup_time = datetime.now().timestamp()

    def emit(self, record):
        """
        Emit a log record with size monitoring
        """
        try:
            # Check if we need to do periodic cleanup
            current_time = datetime.now().timestamp()
            if current_time - self.last_cleanup_time > 3600:  # Every hour
                self._periodic_cleanup()
                self.last_cleanup_time = current_time

            # Emit the record
            super().emit(record)

        except Exception:
            self.handleError(record)

    def _periodic_cleanup(self):
        """
        Perform periodic cleanup of log directory
        """
        try:
            # Get total size of all log files
            total_size = 0
            log_files = []

            for file_path in self.log_dir.glob("*.log*"):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    total_size += size
                    log_files.append((file_path, size))

            # If total size exceeds 50MB, remove oldest files
            if total_size > 52428800:  # 50MB
                # Sort by modification time (oldest first)
                log_files.sort(key=lambda x: x[0].stat().st_mtime)

                # Remove oldest files until we're under 30MB
                target_size = 31457280  # 30MB
                for file_path, size in log_files:
                    if total_size <= target_size:
                        break
                    try:
                        file_path.unlink()
                        total_size -= size
                    except Exception as e:
                        print(f"Error removing {file_path}: {e}")

        except Exception as e:
            print(f"Error during periodic cleanup: {e}")


def create_optimized_handler(filename: str, max_bytes: int = 5242880) -> SizeLimitedFileHandler:
    """
    Create an optimized file handler with 5MB limit

    Args:
        filename: Log file path
        max_bytes: Maximum file size in bytes (default 5MB)

    Returns:
        SizeLimitedFileHandler instance
    """
    return SizeLimitedFileHandler(
        filename=filename,
        maxBytes=max_bytes,
        backupCount=10,
        encoding='utf8'
    )
