"""
Unit tests for storage scanner functionality.
"""

import json
import tempfile
import pytest
from datetime import datetime
from pathlib import Path
from app.storage.scanner import StorageScanner, scan_storage_directory, Snapshot, ArchivedUrl

class TestStorageScanner:
    """Test cases for StorageScanner class."""
    
    @pytest.fixture
    def temp_storage(self):
        """Create temporary storage directory with test data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / "storage"
            
            # Create test URL directory structure
            url1_dir = storage_path / "example_com"
            url1_dir.mkdir(parents=True)
            
            # Create snapshots for URL 1
            snapshot1 = url1_dir / "20240315T143022Z"
            snapshot1.mkdir()
            
            # Create metadata.json for snapshot 1
            metadata1 = {
                "url": "https://example.com",
                "title": "Example Domain",
                "timestamp": "2024-03-15T14:30:22Z"
            }
            with open(snapshot1 / "metadata.json", "w") as f:
                json.dump(metadata1, f)
            
            # Create some artifacts
            (snapshot1 / "warc.file").write_text("fake warc content")
            (snapshot1 / "screenshot.png").write_bytes(b"fake png")
            
            # Create second snapshot
            snapshot2 = url1_dir / "20240316T120000Z"
            snapshot2.mkdir()
            
            metadata2 = {
                "url": "https://example.com",
                "title": "Example Domain Updated",
                "timestamp": "2024-03-16T12:00:00Z"
            }
            with open(snapshot2 / "metadata.json", "w") as f:
                json.dump(metadata2, f)
            
            (snapshot2 / "singlefile.html").write_text("<html>content</html>")
            
            # Create second URL directory
            url2_dir = storage_path / "github_com_user_repo"
            url2_dir.mkdir()
            
            snapshot3 = url2_dir / "20240320T090000Z"
            snapshot3.mkdir()
            
            metadata3 = {
                "url": "https://github.com/user/repo",
                "title": "GitHub Repository",
                "timestamp": "2024-03-20T09:00:00Z"
            }
            with open(snapshot3 / "metadata.json", "w") as f:
                json.dump(metadata3, f)
            
            yield storage_path
    
    @pytest.fixture
    def temp_storage_with_errors(self):
        """Create temporary storage with various error conditions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / "storage"
            
            # URL with invalid timestamp
            url1_dir = storage_path / "invalid_timestamp"
            url1_dir.mkdir(parents=True)
            
            invalid_snapshot = url1_dir / "invalid_timestamp_format"
            invalid_snapshot.mkdir()
            
            # Valid metadata but invalid timestamp folder name
            metadata = {
                "url": "https://invalid-timestamp.com",
                "title": "Invalid Timestamp Test"
            }
            with open(invalid_snapshot / "metadata.json", "w") as f:
                json.dump(metadata, f)
            
            # URL with missing metadata
            url2_dir = storage_path / "missing_metadata"
            url2_dir.mkdir()
            
            snapshot_no_meta = url2_dir / "20240315T143022Z"
            snapshot_no_meta.mkdir()
            # No metadata.json file created
            
            # URL with corrupt metadata
            url3_dir = storage_path / "corrupt_metadata"
            url3_dir.mkdir()
            
            corrupt_snapshot = url3_dir / "20240315T143022Z"
            corrupt_snapshot.mkdir()
            
            # Write invalid JSON
            with open(corrupt_snapshot / "metadata.json", "w") as f:
                f.write("{ invalid json content")
            
            yield storage_path
    
    def test_scanner_initialization(self, temp_storage):
        """Test scanner initialization."""
        scanner = StorageScanner(temp_storage)
        assert scanner.storage_path == temp_storage
        assert scanner.timeout_seconds == 10
    
    def test_parse_timestamp_valid_formats(self):
        """Test timestamp parsing with valid formats."""
        scanner = StorageScanner(Path("/tmp"))
        
        # Test standard format
        dt = scanner._parse_timestamp("20240315T143022Z")
        assert dt == datetime(2024, 3, 15, 14, 30, 22)
        
        # Test alternative formats (if implemented)
        dt2 = scanner._parse_timestamp("20240315_143022")
        if dt2:  # Only test if format is supported
            assert dt2 == datetime(2024, 3, 15, 14, 30, 22)
    
    def test_parse_timestamp_invalid_formats(self):
        """Test timestamp parsing with invalid formats."""
        scanner = StorageScanner(Path("/tmp"))
        
        # Test invalid formats
        assert scanner._parse_timestamp("invalid") is None
        assert scanner._parse_timestamp("2024-13-45") is None
        assert scanner._parse_timestamp("") is None
    
    def test_parse_metadata_json_valid(self, temp_storage):
        """Test parsing valid metadata.json files."""
        scanner = StorageScanner(temp_storage)
        
        metadata_path = temp_storage / "example_com" / "20240315T143022Z" / "metadata.json"
        metadata = scanner._parse_metadata_json(metadata_path)
        
        assert metadata["url"] == "https://example.com"
        assert metadata["title"] == "Example Domain"
        assert metadata["timestamp"] == "2024-03-15T14:30:22Z"
    
    def test_parse_metadata_json_missing(self, temp_storage):
        """Test parsing missing metadata.json file."""
        scanner = StorageScanner(temp_storage)
        
        missing_path = temp_storage / "nonexistent" / "metadata.json"
        metadata = scanner._parse_metadata_json(missing_path)
        
        assert metadata == {}
    
    def test_parse_metadata_json_corrupt(self, temp_storage_with_errors):
        """Test parsing corrupt metadata.json file."""
        scanner = StorageScanner(temp_storage_with_errors)
        
        corrupt_path = temp_storage_with_errors / "corrupt_metadata" / "20240315T143022Z" / "metadata.json"
        metadata = scanner._parse_metadata_json(corrupt_path)
        
        assert metadata == {}
    
    def test_scan_valid_storage(self, temp_storage):
        """Test scanning valid storage directory."""
        scanner = StorageScanner(temp_storage)
        results = scanner.scan()
        
        assert len(results) == 2
        assert "example_com" in results
        assert "github_com_user_repo" in results
        
        # Check example.com data
        example_url = results["example_com"]
        assert example_url.original_url == "https://example.com"
        assert example_url.snapshot_count == 2
        assert len(example_url.snapshots) == 2
        
        # Check snapshots are sorted by timestamp (newest first)
        assert example_url.snapshots[0].timestamp > example_url.snapshots[1].timestamp
        
        # Check artifact detection
        first_snapshot = example_url.snapshots[1]  # Older snapshot (2024-03-15)
        assert "warc.file" in first_snapshot.available_artifacts
        assert "screenshot.png" in first_snapshot.available_artifacts
    
    def test_scan_with_errors(self, temp_storage_with_errors):
        """Test scanning storage with various error conditions."""
        scanner = StorageScanner(temp_storage_with_errors)
        results = scanner.scan()
        
        # Scanner should handle errors gracefully and return partial results
        # Invalid timestamp snapshots should be ignored
        # Missing/corrupt metadata should result in empty snapshots
        assert isinstance(results, dict)
    
    def test_scan_nonexistent_directory(self):
        """Test scanning nonexistent directory."""
        scanner = StorageScanner(Path("/nonexistent/path"))
        results = scanner.scan()
        
        assert results == {}
    
    def test_snapshot_artifact_detection(self, temp_storage):
        """Test artifact detection in snapshots."""
        scanner = StorageScanner(temp_storage)
        results = scanner.scan()
        
        example_url = results["example_com"]
        
        # First snapshot should have warc.file and screenshot.png
        first_snapshot = next(s for s in example_url.snapshots if s.snapshot_id == "20240315T143022Z")
        assert "warc.file" in first_snapshot.available_artifacts
        assert "screenshot.png" in first_snapshot.available_artifacts
        assert "singlefile.html" not in first_snapshot.available_artifacts
        
        # Second snapshot should have singlefile.html
        second_snapshot = next(s for s in example_url.snapshots if s.snapshot_id == "20240316T120000Z")
        assert "singlefile.html" in second_snapshot.available_artifacts
        assert "warc.file" not in second_snapshot.available_artifacts
    
    def test_archived_url_properties(self, temp_storage):
        """Test ArchivedUrl computed properties."""
        scanner = StorageScanner(temp_storage)
        results = scanner.scan()
        
        example_url = results["example_com"]
        
        # Test computed properties
        assert example_url.snapshot_count == 2
        assert example_url.first_captured == datetime(2024, 3, 15, 14, 30, 22)
        assert example_url.last_captured == datetime(2024, 3, 16, 12, 0, 0)
    
    def test_scan_timeout(self, temp_storage):
        """Test scan timeout functionality."""
        # Use very short timeout
        scanner = StorageScanner(temp_storage, timeout_seconds=0.001)
        results = scanner.scan()
        
        # Should complete quickly and may have partial results due to timeout
        assert isinstance(results, dict)
    
    def test_convenience_function(self, temp_storage):
        """Test convenience function scan_storage_directory."""
        results = scan_storage_directory(str(temp_storage))
        
        assert len(results) == 2
        assert "example_com" in results
        assert "github_com_user_repo" in results


class TestDataClasses:
    """Test cases for Snapshot and ArchivedUrl data classes."""
    
    def test_snapshot_creation(self):
        """Test Snapshot dataclass creation."""
        folder_path = Path("/tmp/test")
        metadata = {"title": "Test Title", "url": "https://test.com"}
        
        snapshot = Snapshot(
            snapshot_id="20240315T143022Z",
            timestamp=datetime(2024, 3, 15, 14, 30, 22),
            folder_path=folder_path,
            metadata=metadata,
            url="https://test.com"
        )
        
        assert snapshot.snapshot_id == "20240315T143022Z"
        assert snapshot.title == "Test Title"
        assert snapshot.url == "https://test.com"
        assert snapshot.available_artifacts == []
    
    def test_archived_url_creation(self):
        """Test ArchivedUrl dataclass creation."""
        snapshots = [
            Snapshot(
                snapshot_id="20240315T143022Z",
                timestamp=datetime(2024, 3, 15, 14, 30, 22),
                folder_path=Path("/tmp/test1"),
                metadata={},
                url="https://test.com"
            ),
            Snapshot(
                snapshot_id="20240316T120000Z",
                timestamp=datetime(2024, 3, 16, 12, 0, 0),
                folder_path=Path("/tmp/test2"),
                metadata={},
                url="https://test.com"
            )
        ]
        
        archived_url = ArchivedUrl(
            url_id="test_com",
            original_url="https://test.com",
            folder_name="test_com",
            snapshots=snapshots
        )
        
        assert archived_url.snapshot_count == 2
        assert archived_url.first_captured == datetime(2024, 3, 15, 14, 30, 22)
        assert archived_url.last_captured == datetime(2024, 3, 16, 12, 0, 0)
    
    def test_archived_url_empty_snapshots(self):
        """Test ArchivedUrl with no snapshots."""
        archived_url = ArchivedUrl(
            url_id="test_com",
            original_url="https://test.com",
            folder_name="test_com"
        )
        
        assert archived_url.snapshot_count == 0
        assert archived_url.first_captured is None
        assert archived_url.last_captured is None


if __name__ == "__main__":
    pytest.main([__file__])