"""
Filesystem storage provider implementation.

This module contains the FilesystemStorageProvider that implements storage
operations for local filesystem archives with integrated scanning functionality.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, IO
from urllib.parse import unquote

from .base import StorageProvider, StorageError
from ...models.url import ArchivedUrl
from ...models.snapshot import Snapshot

logger = logging.getLogger(__name__)


class FilesystemStorageProvider(StorageProvider):
    """
    Filesystem implementation of StorageProvider.
    
    This provider handles storage operations for archives stored in the
    local filesystem with integrated directory scanning functionality.
    """

    def __init__(self, storage_path: Path, timeout_seconds: int = 10):
        """
        Initialize filesystem storage provider.
        
        Args:
            storage_path: Path to the archives directory
            timeout_seconds: Maximum time to spend on operations
        """
        self.storage_path = Path(storage_path)
        self.timeout_seconds = timeout_seconds
        self._cached_results: Optional[Dict[str, ArchivedUrl]] = None
        self._start_time = None

    def _check_timeout(self) -> bool:
        """Check if scanning has exceeded timeout."""
        if self._start_time is None:
            return False
        return time.time() - self._start_time > self.timeout_seconds

    def _parse_timestamp(self, folder_name: str) -> Optional[datetime]:
        """
        Parse timestamp string from request folder name.
        
        Expected format: req_{request_id}_{YYYYMMDD_HHMMSS}
        """
        try:
            # Extract timestamp from request folder format: req_{request_id}_{YYYYMMDD_HHMMSS}
            if folder_name.startswith('req_'):
                parts = folder_name.split('_')
                if len(parts) >= 3:
                    # Get the last two parts as date and time
                    date_part = parts[-2]  # YYYYMMDD
                    time_part = parts[-1]  # HHMMSS
                    timestamp_str = f"{date_part}_{time_part}"
                    
                    # Try to parse the extracted timestamp
                    try:
                        return datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    except ValueError:
                        pass
            
            formats = [
                '%Y%m%dT%H%M%SZ',  # 20240315T143022Z
                '%Y%m%d_%H%M%S',   # 20240315_143022
                '%Y-%m-%d_%H-%M-%S' # 2024-03-15_14-30-22
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(folder_name, fmt)
                except ValueError:
                    continue
            
            logger.warning(f"Could not parse timestamp from folder: {folder_name}")
            return None
            
        except Exception as e:
            logger.warning(f"Error parsing timestamp from {folder_name}: {e}")
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
            
            # Extract URL from metadata.archive_info.url (new structure)
            url = ''
            if metadata and 'archive_info' in metadata:
                url = metadata['archive_info'].get('url', '')
            elif metadata:
                url = metadata.get('url', '')
            
            if not url:
                # Try to reconstruct URL from url_id (URL-decoded)
                try:
                    url = unquote(url_id.replace('_', '/'))
                    if not url.startswith(('http://', 'https://')):
                        url = f'https://{url}'
                except Exception:
                    url = f'https://{url_id}'
            
            # Check for available artifacts
            artifact_files = ['archive.wacz', 'metadata.json', 'screenshot.png', 'singlefile.html']
            available_artifacts = []
            
            for artifact in artifact_files:
                if (snapshot_dir / artifact).exists():
                    available_artifacts.append(artifact)
            
            return Snapshot(
                snapshot_id=snapshot_id,
                timestamp=timestamp,
                url=url,
                title=metadata.get('title', ''),
                folder_path=str(snapshot_dir),  # Convert Path to string for Pydantic
                metadata=metadata,
                available_artifacts=available_artifacts
            )
            
        except Exception as e:
            logger.error(f"Error scanning snapshot directory {snapshot_dir}: {e}")
            return None

    def _scan_path_directory(self, path_dir: Path, domain: str) -> list[Snapshot]:
        """
        Scan a path directory for request-timestamp snapshot folders.
        
        Args:
            path_dir: Path to path segment directory (e.g., archives/example_com/home_page)
            domain: Domain name for URL construction
            
        Returns:
            List of Snapshot objects found in this path directory
        """
        snapshots = []
        path_segment = path_dir.name
        url_id = f"{domain}_{path_segment}"
        
        try:
            # Scan for request-timestamp snapshot subdirectories
            for item in path_dir.iterdir():
                if self._check_timeout():
                    logger.warning(f"Timeout reached while scanning {path_dir}")
                    break
                
                if not item.is_dir():
                    continue
                
                # Only process directories that start with 'req_'
                if not item.name.startswith('req_'):
                    logger.debug(f"Skipping non-request directory: {item.name}")
                    continue
                
                snapshot = self._scan_snapshot_directory(item, url_id)
                if snapshot:
                    snapshots.append(snapshot)
        
        except Exception as e:
            logger.error(f"Error scanning path directory {path_dir}: {e}")
        
        return snapshots

    def _scan_domain_directory(self, domain_dir: Path) -> list[ArchivedUrl]:
        """
        Scan a domain directory for path subdirectories.
        
        Args:
            domain_dir: Path to domain directory (e.g., archives/example_com)
            
        Returns:
            List of ArchivedUrl objects found in this domain
        """
        archived_urls = []
        domain = domain_dir.name
        
        try:
            # Scan for path subdirectories
            for path_dir in domain_dir.iterdir():
                if self._check_timeout():
                    logger.warning(f"Timeout reached while scanning domain {domain_dir}")
                    break
                
                if not path_dir.is_dir():
                    continue
                
                # Get snapshots for this path
                snapshots = self._scan_path_directory(path_dir, domain)
                
                if not snapshots:
                    logger.debug(f"No valid snapshots found in {path_dir}")
                    continue
                
                # Sort snapshots by timestamp (newest first)
                snapshots.sort(key=lambda s: s.timestamp, reverse=True)
                
                # Create URL ID from domain and path
                url_id = f"{domain}_{path_dir.name}"
                
                # Get original URL from first snapshot
                original_url = snapshots[0].url if snapshots else f"https://{domain.replace('_', '.')}"
                
                archived_url = ArchivedUrl(
                    url_id=url_id,
                    original_url=original_url,
                    folder_name=f"{domain}/{path_dir.name}",
                    snapshots=snapshots
                )
                
                archived_urls.append(archived_url)
        
        except Exception as e:
            logger.error(f"Error scanning domain directory {domain_dir}: {e}")
        
        return archived_urls

    def _scan_storage(self) -> Dict[str, ArchivedUrl]:
        """
        Scan the storage directory for all archived URLs and snapshots using three-level hierarchy.
        
        Structure: archives/domain/path_segment/req_request-id_timestamp/
        
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
            
            logger.info(f"Scanning archives directory: {self.storage_path}")
            
            # Scan each domain directory (level 1)
            for domain_dir in self.storage_path.iterdir():
                if self._check_timeout():
                    logger.warning("Scan timeout reached")
                    break
                
                if not domain_dir.is_dir():
                    continue
                
                # Get all archived URLs for this domain
                domain_urls = self._scan_domain_directory(domain_dir)
                
                # Add each archived URL to results
                for archived_url in domain_urls:
                    archived_urls[archived_url.url_id] = archived_url
            
            scan_duration = time.time() - self._start_time
            total_snapshots = sum(url.snapshot_count for url in archived_urls.values())
            logger.info(f"Scan completed in {scan_duration:.2f}s. Found {len(archived_urls)} URLs with {total_snapshots} total snapshots")
            
            return archived_urls
            
        except Exception as e:
            logger.error(f"Error during storage scan: {e}")
            return archived_urls
        
    def get_all_urls(self) -> Dict[str, ArchivedUrl]:
        """
        Get all archived URLs from filesystem.
        
        Returns:
            Dictionary mapping url_id to ArchivedUrl objects
            
        Raises:
            StorageError: If filesystem scan fails
        """
        try:
            # Perform direct filesystem scan
            archived_urls = self._scan_storage()
            self._cached_results = archived_urls
            return archived_urls
            
        except Exception as e:
            logger.error(f"Error scanning filesystem storage: {e}")
            raise StorageError(f"Failed to scan filesystem storage: {str(e)}") from e

    def get_url_by_id(self, url_id: str) -> Optional[ArchivedUrl]:
        """
        Get a specific archived URL by its ID.
        
        Args:
            url_id: The URL identifier
            
        Returns:
            ArchivedUrl object if found, None otherwise
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            # Get all URLs if not cached
            if self._cached_results is None:
                self.get_all_urls()
            
            return self._cached_results.get(url_id) if self._cached_results else None
            
        except Exception as e:
            logger.error(f"Error getting URL {url_id}: {e}")
            raise StorageError(f"Failed to get URL {url_id}: {str(e)}") from e

    def get_snapshot_by_id(self, snapshot_id: str) -> Optional[Snapshot]:
        """
        Get a specific snapshot by its ID.
        
        Args:
            snapshot_id: The snapshot identifier
            
        Returns:
            Snapshot object if found, None otherwise
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            # Get all URLs to search for snapshot
            if self._cached_results is None:
                self.get_all_urls()
            
            if not self._cached_results:
                return None
                
            # Search through all URLs for the snapshot
            for archived_url in self._cached_results.values():
                for snapshot in archived_url.snapshots:
                    if snapshot.snapshot_id == snapshot_id:
                        return snapshot
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting snapshot {snapshot_id}: {e}")
            raise StorageError(f"Failed to get snapshot {snapshot_id}: {str(e)}") from e

    def get_artifact_stream(self, snapshot_id: str, artifact_type: str) -> Optional[IO]:
        """
        Get a stream to a specific artifact file.
        
        Args:
            snapshot_id: The snapshot identifier
            artifact_type: Type of artifact (e.g., 'archive.wacz', 'screenshot.png')
            
        Returns:
            File-like object stream if found, None otherwise
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            # Get snapshot to find file path
            snapshot = self.get_snapshot_by_id(snapshot_id)
            if not snapshot:
                return None
            
            # Build artifact file path (convert string back to Path)
            artifact_path = Path(snapshot.folder_path) / artifact_type
            
            if not artifact_path.exists():
                logger.debug(f"Artifact not found: {artifact_path}")
                return None
            
            # Open and return file stream
            return open(artifact_path, 'rb')
            
        except Exception as e:
            logger.error(f"Error getting artifact stream {snapshot_id}/{artifact_type}: {e}")
            raise StorageError(f"Failed to get artifact stream: {str(e)}") from e

    def artifact_exists(self, snapshot_id: str, artifact_type: str) -> bool:
        """
        Check if a specific artifact exists.
        
        Args:
            snapshot_id: The snapshot identifier  
            artifact_type: Type of artifact (e.g., 'archive.wacz', 'screenshot.png')
            
        Returns:
            True if artifact exists, False otherwise
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            # Get snapshot to find file path
            snapshot = self.get_snapshot_by_id(snapshot_id)
            if not snapshot:
                return False
            
            # Check if artifact file exists
            artifact_path = Path(snapshot.folder_path) / artifact_type
            return artifact_path.exists() and artifact_path.is_file()
            
        except Exception as e:
            logger.error(f"Error checking artifact existence {snapshot_id}/{artifact_type}: {e}")
            raise StorageError(f"Failed to check artifact existence: {str(e)}") from e

    def get_artifact_path(self, snapshot_id: str, artifact_type: str) -> Optional[Path]:
        """
        Get the path to a specific artifact file.
        
        Args:
            snapshot_id: The snapshot identifier
            artifact_type: Type of artifact (e.g., 'archive.wacz', 'screenshot.png')
            
        Returns:
            Path to artifact file if exists, None otherwise
            
        Raises:
            StorageError: If storage operation fails
        """
        try:
            # Get snapshot to find file path
            snapshot = self.get_snapshot_by_id(snapshot_id)
            if not snapshot:
                return None
            
            # Build and validate artifact file path
            artifact_path = Path(snapshot.folder_path) / artifact_type
            
            if not artifact_path.exists():
                return None
                
            return artifact_path
            
        except Exception as e:
            logger.error(f"Error getting artifact path {snapshot_id}/{artifact_type}: {e}")
            raise StorageError(f"Failed to get artifact path: {str(e)}") from e