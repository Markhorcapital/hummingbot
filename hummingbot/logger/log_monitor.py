#!/usr/bin/env python3
"""
Log Monitor

This module provides utilities to monitor log file sizes and ensure they stay within 5MB limits.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List


class LogMonitor:
    """
    Monitor log file sizes and provide cleanup utilities
    """

    def __init__(self, logs_dir: str = "logs", max_file_size_mb: int = 5):
        self.logs_dir = Path(logs_dir)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.logs_dir.mkdir(exist_ok=True)

    def get_log_files(self) -> List[Path]:
        """
        Get all log files in the logs directory
        """
        return list(self.logs_dir.glob("*.log*"))

    def get_file_size(self, file_path: Path) -> int:
        """
        Get file size in bytes
        """
        try:
            return file_path.stat().st_size
        except (OSError, FileNotFoundError):
            return 0

    def get_file_size_mb(self, file_path: Path) -> float:
        """
        Get file size in MB
        """
        return self.get_file_size(file_path) / (1024 * 1024)

    def check_file_sizes(self) -> Dict[str, Dict]:
        """
        Check all log file sizes and return status
        """
        status = {
            "total_files": 0,
            "total_size_mb": 0.0,
            "oversized_files": [],
            "files": []
        }

        for log_file in self.get_log_files():
            size_bytes = self.get_file_size(log_file)
            size_mb = size_bytes / (1024 * 1024)

            file_info = {
                "name": log_file.name,
                "size_bytes": size_bytes,
                "size_mb": round(size_mb, 2),
                "is_oversized": size_bytes > self.max_file_size_bytes,
                "last_modified": datetime.fromtimestamp(log_file.stat().st_mtime).isoformat()
            }

            status["files"].append(file_info)
            status["total_files"] += 1
            status["total_size_mb"] += size_mb

            if file_info["is_oversized"]:
                status["oversized_files"].append(file_info)

        status["total_size_mb"] = round(status["total_size_mb"], 2)
        return status

    def cleanup_oversized_files(self) -> int:
        """
        Clean up files that exceed the size limit
        Returns number of files cleaned up
        """
        cleaned_count = 0

        for log_file in self.get_log_files():
            if self.get_file_size(log_file) > self.max_file_size_bytes:
                try:
                    # Compress the file instead of deleting
                    self._compress_file(log_file)
                    cleaned_count += 1
                except Exception as e:
                    logging.error(f"Failed to compress {log_file}: {e}")

        return cleaned_count

    def _compress_file(self, file_path: Path):
        """
        Compress a file using gzip
        """
        import gzip
        import shutil

        compressed_path = file_path.with_suffix(file_path.suffix + '.gz')

        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Remove original file after successful compression
        file_path.unlink()

    def get_size_summary(self) -> str:
        """
        Get a summary of log file sizes
        """
        status = self.check_file_sizes()

        summary = f"Log Directory: {self.logs_dir}\n"
        summary += f"Total Files: {status['total_files']}\n"
        summary += f"Total Size: {status['total_size_mb']} MB\n"
        summary += f"Oversized Files: {len(status['oversized_files'])}\n\n"

        if status['oversized_files']:
            summary += "Oversized Files:\n"
            for file_info in status['oversized_files']:
                summary += f"  - {file_info['name']}: {file_info['size_mb']} MB\n"

        summary += "\nAll Files:\n"
        for file_info in sorted(status['files'], key=lambda x: x['size_mb'], reverse=True):
            status_icon = "⚠️" if file_info['is_oversized'] else "✅"
            summary += f"  {status_icon} {file_info['name']}: {file_info['size_mb']} MB\n"

        return summary

    def enforce_size_limit(self) -> bool:
        """
        Enforce the 5MB size limit by cleaning up oversized files
        Returns True if cleanup was performed
        """
        status = self.check_file_sizes()

        if status['oversized_files']:
            logging.warning(f"Found {len(status['oversized_files'])} oversized log files. Cleaning up...")
            cleaned = self.cleanup_oversized_files()
            logging.info(f"Cleaned up {cleaned} oversized log files")
            return True

        return False


def monitor_logs(logs_dir: str = "logs") -> None:
    """
    Monitor log files and print status
    """
    monitor = LogMonitor(logs_dir)
    print(monitor.get_size_summary())

    if monitor.enforce_size_limit():
        print("\n✅ Size limit enforced. Log files cleaned up.")
    else:
        print("\n✅ All log files are within size limits.")


if __name__ == "__main__":
    import sys

    logs_dir = sys.argv[1] if len(sys.argv) > 1 else "logs"
    monitor_logs(logs_dir)
