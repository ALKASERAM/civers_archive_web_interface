"""
Storage scanner for discovering and parsing archived web content.

This module provides functionality to scan the filesystem storage structure,
discover archived URLs and snapshots, and parse metadata.json files.
"""

import json
import logging
import os
import time
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

logger = logging.getLogger(__name__)

@dataclass
class Snapshot:
    """Represents a single snapshot of a URL."""
    snapshot_id: str
    timestamp: datetime
    folder_path: Path
    metadata: Dict
    url: str
    title: Optional[str] = None
    available_artifacts: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Extract title from metadata and check for available artifacts."""
        if not self.title and self.metadata:
            self.title = self.metadata.get('title', '')
        
        # Check for standard artifact types
        artifact_files = ['warc.file', 'screenshot.png', 'singlefile.html', 'document.html']
        self.available_artifacts = []
        
        for artifact in artifact_files:
            if (self.folder_path / artifact).exists():
                self.available_artifacts.append(artifact)

@dataclass
class ArchivedUrl:
    """Represents an archived URL with its snapshots."""
    url_id: str
    original_url: str
    folder_name: str
    snapshots: List[Snapshot] = field(default_factory=list)
    
    @property
    def snapshot_count(self) -> int:
        return len(self.snapshots)
    
    @property
    def first_captured(self) -> Optional[datetime]:
        if not self.snapshots:
            return None
        return min(snapshot.timestamp for snapshot in self.snapshots)
    
    @property
    def last_captured(self) -> Optional[datetime]:
        if not self.snapshots:
            return None
        return max(snapshot.timestamp for snapshot in self.snapshots)

class StorageScanner:
    """Scanner for archived web content in filesystem storage."""
    
    def __init__(self, storage_path: Path, timeout_seconds: int = 10):
        """
        Initialize storage scanner.
        
        Args:
            storage_path: Path to the storage directory
            timeout_seconds: Maximum time to spend scanning
        """
        self.storage_path = Path(storage_path)
        self.timeout_seconds = timeout_seconds
        self._start_time = None
    
    def _check_timeout(self) -> bool:
        """Check if scanning has exceeded timeout."""
        if self._start_time is None:
            return False
        return time.time() - self._start_time > self.timeout_seconds
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string from folder name.
        
        Expected format: YYYYMMDDTHHMMSSZ
        """
        try:
            # Handle different timestamp formats
            formats = [
                '%Y%m%dT%H%M%SZ',  # 20240315T143022Z
                '%Y%m%d_%H%M%S',   # 20240315_143022
                '%Y-%m-%d_%H-%M-%S' # 2024-03-15_14-30-22
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp_str, fmt)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse timestamp: {timestamp_str}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing timestamp {timestamp_str}: {e}")
            return None
    
    def _parse_metadata_json(self, metadata_path: Path) -> Dict:
        """
        Parse metadata.json file with error handling.
        
        Args:
            metadata_path: Path to metadata.json file
            
        Returns:
            Dictionary containing metadata, or empty dict if parsing fails
        """
        try:
            if not metadata_path.exists():
                logger.debug(f"Metadata file not found: {metadata_path}")
                return {}
            
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Validate required fields
            if not isinstance(metadata, dict):
                logger.warning(f"Invalid metadata format in {metadata_path}")
                return {}
            
            return metadata
            
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {metadata_path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error reading metadata {metadata_path}: {e}")
            return {}
    
    def _scan_snapshot_directory(self, snapshot_dir: Path, url_id: str) -> Optional[Snapshot]:
        """
        Scan a single snapshot directory.
        
        Args:
            snapshot_dir: Path to snapshot directory
            url_id: URL identifier
            
        Returns:
            Snapshot object or None if parsing fails
        """
        try:
            snapshot_id = snapshot_dir.name
            
            # Parse timestamp from directory name
            timestamp = self._parse_timestamp(snapshot_id)
            if not timestamp:
                logger.warning(f"Could not parse timestamp for snapshot: {snapshot_dir}")
                return None
            
            # Parse metadata.json
            metadata_path = snapshot_dir / 'metadata.json'
            metadata = self._parse_metadata_json(metadata_path)
            
            # Extract URL from metadata or reconstruct from url_id
            url = metadata.get('url', '')
            if not url:
                # Try to reconstruct URL from url_id (URL-decoded)
                try:
                    url = unquote(url_id.replace('_', '/'))
                except Exception:
                    url = url_id
            
            return Snapshot(
                snapshot_id=snapshot_id,
                timestamp=timestamp,
                folder_path=snapshot_dir,
                metadata=metadata,
                url=url,
                title=metadata.get('title', '')
            )
            
        except Exception as e:
            logger.error(f"Error scanning snapshot directory {snapshot_dir}: {e}")
            return None
    
    def _scan_url_directory(self, url_dir: Path) -> Optional[ArchivedUrl]:
        """
        Scan a single URL directory for snapshots.
        
        Args:
            url_dir: Path to URL directory
            
        Returns:
            ArchivedUrl object or None if no valid snapshots found
        """
        try:
            url_id = url_dir.name
            snapshots = []
            
            # Scan for snapshot subdirectories
            for item in url_dir.iterdir():
                if self._check_timeout():
                    logger.warning(f"Timeout reached while scanning {url_dir}")
                    break
                
                if not item.is_dir():
                    continue
                
                snapshot = self._scan_snapshot_directory(item, url_id)
                if snapshot:
                    snapshots.append(snapshot)
            
            if not snapshots:
                logger.debug(f"No valid snapshots found in {url_dir}")
                return None
            
            # Sort snapshots by timestamp (newest first)
            snapshots.sort(key=lambda s: s.timestamp, reverse=True)
            
            # Get original URL from first snapshot
            original_url = snapshots[0].url if snapshots else url_id
            
            return ArchivedUrl(
                url_id=url_id,
                original_url=original_url,
                folder_name=url_dir.name,
                snapshots=snapshots
            )
            
        except Exception as e:
            logger.error(f"Error scanning URL directory {url_dir}: {e}")
            return None
    
    def scan(self) -> Dict[str, ArchivedUrl]:
        """
        Scan the storage directory for all archived URLs and snapshots.
        
        Returns:
            Dictionary mapping url_id to ArchivedUrl objects
        """
        self._start_time = time.time()
        archived_urls = {}
        
        try:
            if not self.storage_path.exists():
                logger.error(f"Storage path does not exist: {self.storage_path}")
                return archived_urls
            
            if not self.storage_path.is_dir():
                logger.error(f"Storage path is not a directory: {self.storage_path}")
                return archived_urls
            
            logger.info(f"Scanning storage directory: {self.storage_path}")
            
            # Scan each URL directory
            for item in self.storage_path.iterdir():
                if self._check_timeout():
                    logger.warning("Scan timeout reached")
                    break
                
                if not item.is_dir():
                    continue
                
                archived_url = self._scan_url_directory(item)
                if archived_url:
                    archived_urls[archived_url.url_id] = archived_url
            
            scan_duration = time.time() - self._start_time
            logger.info(f"Scan completed in {scan_duration:.2f}s. Found {len(archived_urls)} URLs")
            
            return archived_urls
            
        except Exception as e:
            logger.error(f"Error during storage scan: {e}")
            return archived_urls

def scan_storage_directory(storage_path: str, timeout_seconds: int = 10) -> Dict[str, ArchivedUrl]:
    """
    Convenience function to scan storage directory.
    
    Args:
        storage_path: Path to storage directory
        timeout_seconds: Maximum time to spend scanning
        
    Returns:
        Dictionary mapping url_id to ArchivedUrl objects
    """
    scanner = StorageScanner(Path(storage_path), timeout_seconds)
    return scanner.scan()

# CLI support for testing
if __name__ == "__main__":
    import sys
    import argparse
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description='Scan storage directory for archived URLs')
    parser.add_argument('storage_path', help='Path to storage directory')
    parser.add_argument('--timeout', type=int, default=10, help='Scan timeout in seconds')
    
    args = parser.parse_args()
    
    # Perform scan
    results = scan_storage_directory(args.storage_path, args.timeout)
    
    # Print results
    print(f"Found {len(results)} archived URLs:")
    for url_id, archived_url in results.items():
        print(f"  {url_id}: {archived_url.original_url} ({archived_url.snapshot_count} snapshots)")
        for snapshot in archived_url.snapshots[:3]:  # Show first 3 snapshots
            print(f"    - {snapshot.timestamp} ({len(snapshot.available_artifacts)} artifacts)")