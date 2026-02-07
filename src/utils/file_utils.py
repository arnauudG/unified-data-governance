"""
File utility functions.

This module provides utilities for file operations, including
finding latest files and cleaning up old files.
"""

import re
from pathlib import Path
from typing import Optional, List

from src.core.logging import get_logger
from src.core.constants import FilePatterns

logger = get_logger(__name__)


def find_latest_file(pattern: str, directory: Path) -> Optional[Path]:
    """
    Find the latest file matching a pattern in a directory.

    Args:
        pattern: Glob pattern to match (e.g., "datasets_*.csv")
        directory: Directory to search in

    Returns:
        Path to latest file, or None if not found
    """
    if not directory.exists():
        return None

    files = list(directory.glob(pattern))
    if not files:
        return None

    def get_timestamp_from_filename(filepath: Path) -> Optional[str]:
        """Extract timestamp from filename for sorting."""
        filename = filepath.name
        match = re.search(r"(\d{8}_\d{6})", filename)
        if match:
            return match.group(1)
        return None

    def sort_key(filepath: Path) -> tuple:
        """Sort key: timestamp if available, else modification time."""
        timestamp = get_timestamp_from_filename(filepath)
        if timestamp:
            return (0, timestamp)  # Prefer timestamped files
        else:
            return (1, filepath.stat().st_mtime)

    try:
        files.sort(key=sort_key, reverse=True)
        return files[0]
    except Exception as e:
        logger.warning(f"Error sorting files: {e}")
        # Fallback to modification time
        return max(files, key=lambda f: f.stat().st_mtime)


def cleanup_old_files(
    directory: Path,
    patterns_to_remove: Optional[List[str]] = None,
    files_to_keep: Optional[List[str]] = None,
) -> int:
    """
    Remove old files matching patterns, keeping specified files.

    Args:
        directory: Directory to clean up
        patterns_to_remove: List of glob patterns for files to remove
        files_to_keep: List of filenames to keep

    Returns:
        Number of files removed
    """
    if patterns_to_remove is None:
        patterns_to_remove = FilePatterns.PATTERNS_TO_REMOVE

    if files_to_keep is None:
        files_to_keep = FilePatterns.FILES_TO_KEEP

    removed_count = 0

    if not directory.exists():
        return removed_count

    for pattern in patterns_to_remove:
        files = list(directory.glob(pattern))
        for file_path in files:
            filename = file_path.name

            # Skip files we want to keep
            if filename in files_to_keep:
                continue

            try:
                file_path.unlink()
                logger.info(f"🗑️  Removed old file: {filename}")
                removed_count += 1
            except Exception as e:
                logger.warning(f"⚠️  Could not remove {filename}: {e}")

    return removed_count
