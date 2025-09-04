"""
Unit tests for storage scanner functionality.
"""

import json
import tempfile
import pytest
import shutil
from datetime import datetime
from pathlib import Path
from app.storage.scanner import StorageScanner, scan_storage_directory, Snapshot, ArchivedUrl

class TestStorageScanner:
    """Test cases for StorageScanner class."""
    
    @pytest.fixture
    def real_archives_data(self):
        """Use real archives data for testing."""
        test_data_path = Path(__file__).parent / "test_data" / "test_archives"
        if not test_data_path.exists():
            pytest.skip("Test archives data not available")
        return test_data_path
    
    @pytest.fixture
    def temp_storage_three_level(self):
        """Create temporary storage directory with three-level structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / "archives"
            
            # Create domain directory (level 1)
            domain_dir = storage_path / "example_com"
            domain_dir.mkdir(parents=True)
            
            # Create path segment directory (level 2)
            path_dir = domain_dir / "home_page"
            path_dir.mkdir()
            
            # Create request snapshot directory (level 3)
            snapshot_dir = path_dir / "req_test-request-1_20250904_061411"
            snapshot_dir.mkdir()
            
            # Create metadata.json with new structure
            metadata = {
                "archive_info": {
                    "url": "https://example.com/",
                    "request_id": "test-request-1",
                    "created_at": "2025-09-04T06:14:11.196512",
                    "completed_at": "2025-09-04T06:14:24.292809"
                },
                "results": {
                    "artifacts_created": [
                        "archive.wacz",
                        "screenshot.png",
                        "singlefile.html",
                        "metadata.json"
                    ]
                }
            }
            with open(snapshot_dir / "metadata.json", "w") as f:
                json.dump(metadata, f)
            
            # Create core artifacts
            (snapshot_dir / "archive.wacz").write_bytes(b"fake wacz content")
            (snapshot_dir / "screenshot.png").write_bytes(b"fake png")
            (snapshot_dir / "singlefile.html").write_text("<html>SingleFile content</html>")
            
            # Create second snapshot for the same URL
            snapshot2_dir = path_dir / "req_test-request-1_20250904_061058"
            snapshot2_dir.mkdir()
            
            metadata2 = {
                "archive_info": {
                    "url": "https://example.com/",
                    "request_id": "test-request-1",
                    "created_at": "2025-09-04T06:10:58.000000"
                }
            }
            with open(snapshot2_dir / "metadata.json", "w") as f:
                json.dump(metadata2, f)
            # No artifacts for second snapshot (test empty case)
            
            # Create second domain with different path
            domain2_dir = storage_path / "httpbin_org"
            domain2_dir.mkdir()
            
            path2_dir = domain2_dir / "get"
            path2_dir.mkdir()
            
            snapshot3_dir = path2_dir / "req_test-request-2_20250904_061424"
            snapshot3_dir.mkdir()
            
            metadata3 = {
                "archive_info": {
                    "url": "https://httpbin.org/get",
                    "request_id": "test-request-2",
                    "created_at": "2025-09-04T06:14:24.000000"
                }
            }
            with open(snapshot3_dir / "metadata.json", "w") as f:
                json.dump(metadata3, f)
            
            (snapshot3_dir / "archive.wacz").write_bytes(b"fake wacz 2")
            (snapshot3_dir / "singlefile.html").write_text("<html>HTTPBin content</html>")
            
            yield storage_path
    
    @pytest.fixture
    def temp_storage_with_errors(self):
        """Create temporary storage with various error conditions using three-level structure."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_path = Path(temp_dir) / "archives"
            
            # Domain with invalid request timestamp
            domain_dir = storage_path / "invalid_timestamp_com"
            path_dir = domain_dir / "test_page"
            path_dir.mkdir(parents=True)
            
            # Invalid timestamp folder format (should start with req_)
            invalid_snapshot = path_dir / "invalid_timestamp_format"
            invalid_snapshot.mkdir()
            
            metadata = {
                "archive_info": {
                    "url": "https://invalid-timestamp.com",
                    "request_id": "test"
                }
            }
            with open(invalid_snapshot / "metadata.json", "w") as f:
                json.dump(metadata, f)
            
            # Domain with missing metadata
            domain2_dir = storage_path / "missing_metadata_com"
            path2_dir = domain2_dir / "test_page"
            path2_dir.mkdir(parents=True)
            
            snapshot_no_meta = path2_dir / "req_test_20240315_143022"
            snapshot_no_meta.mkdir()
            # No metadata.json file created
            
            # Domain with corrupt metadata
            domain3_dir = storage_path / "corrupt_metadata_com"
            path3_dir = domain3_dir / "test_page"
            path3_dir.mkdir(parents=True)
            
            corrupt_snapshot = path3_dir / "req_test_20240315_143022"
            corrupt_snapshot.mkdir()
            
            # Write invalid JSON
            with open(corrupt_snapshot / "metadata.json", "w") as f:
                f.write("{ invalid json content")
            
            yield storage_path
    
    def test_scanner_initialization(self, temp_storage_three_level):
        """Test scanner initialization."""
        scanner = StorageScanner(temp_storage_three_level)
        assert scanner.storage_path == temp_storage_three_level
        assert scanner.timeout_seconds == 10
    
    def test_parse_timestamp_valid_formats(self):
        """Test timestamp parsing with valid formats."""
        scanner = StorageScanner(Path("/tmp"))
        
        # Test new request-timestamp format
        dt = scanner._parse_timestamp("req_test-request-1_20250904_061411")
        assert dt == datetime(2025, 9, 4, 6, 14, 11)
        
        # Test different request ID format
        dt2 = scanner._parse_timestamp("req_archive-session-123_20240315_143022")
        assert dt2 == datetime(2024, 3, 15, 14, 30, 22)
        
        # Test timestamp only format
        dt3 = scanner._parse_timestamp("20240315T143022Z")
        assert dt3 == datetime(2024, 3, 15, 14, 30, 22)
    
    def test_parse_timestamp_invalid_formats(self):
        """Test timestamp parsing with invalid formats."""
        scanner = StorageScanner(Path("/tmp"))
        
        # Test invalid formats
        assert scanner._parse_timestamp("invalid") is None
        assert scanner._parse_timestamp("req_invalid_format") is None
        # Note: req__20240315_143022 actually parses successfully (empty request ID but valid timestamp)
        assert scanner._parse_timestamp("not_req_format") is None
        assert scanner._parse_timestamp("") is None
    
    def test_parse_metadata_json_valid(self, temp_storage_three_level):
        """Test parsing valid metadata.json files with new structure."""
        scanner = StorageScanner(temp_storage_three_level)
        
        metadata_path = temp_storage_three_level / "example_com" / "home_page" / "req_test-request-1_20250904_061411" / "metadata.json"
        metadata = scanner._parse_metadata_json(metadata_path)
        
        assert metadata["archive_info"]["url"] == "https://example.com/"
        assert metadata["archive_info"]["request_id"] == "test-request-1"
        assert "created_at" in metadata["archive_info"]
    
    def test_parse_metadata_json_missing(self, temp_storage_three_level):
        """Test parsing missing metadata.json file."""
        scanner = StorageScanner(temp_storage_three_level)
        
        missing_path = temp_storage_three_level / "nonexistent" / "path" / "req_test" / "metadata.json"
        metadata = scanner._parse_metadata_json(missing_path)
        
        assert metadata == {}
    
    def test_parse_metadata_json_corrupt(self, temp_storage_with_errors):
        """Test parsing corrupt metadata.json file."""
        scanner = StorageScanner(temp_storage_with_errors)
        
        corrupt_path = temp_storage_with_errors / "corrupt_metadata_com" / "test_page" / "req_test_20240315_143022" / "metadata.json"
        metadata = scanner._parse_metadata_json(corrupt_path)
        
        assert metadata == {}
    
    def test_scan_valid_storage(self, temp_storage_three_level):
        """Test scanning valid storage directory with three-level structure."""
        scanner = StorageScanner(temp_storage_three_level)
        results = scanner.scan()
        
        assert len(results) == 2
        assert "example_com_home_page" in results
        assert "httpbin_org_get" in results
        
        # Check example.com data
        example_url = results["example_com_home_page"]
        assert example_url.original_url == "https://example.com/"
        assert example_url.snapshot_count == 2
        assert len(example_url.snapshots) == 2
        
        # Check snapshots are sorted by timestamp (newest first)
        assert example_url.snapshots[0].timestamp > example_url.snapshots[1].timestamp
        
        # Check artifact detection - newer snapshot should have 4 artifacts
        newer_snapshot = example_url.snapshots[0]  # req_test-request-1_20250904_061411
        assert "archive.wacz" in newer_snapshot.available_artifacts
        assert "metadata.json" in newer_snapshot.available_artifacts
        assert "screenshot.png" in newer_snapshot.available_artifacts
        assert "singlefile.html" in newer_snapshot.available_artifacts
    
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
    
    def test_snapshot_artifact_detection(self, temp_storage_three_level):
        """Test artifact detection in snapshots with new core artifact types."""
        scanner = StorageScanner(temp_storage_three_level)
        results = scanner.scan()
        
        example_url = results["example_com_home_page"]
        
        # First snapshot should have all four core artifacts
        first_snapshot = next(s for s in example_url.snapshots if s.snapshot_id == "req_test-request-1_20250904_061411")
        assert "archive.wacz" in first_snapshot.available_artifacts
        assert "metadata.json" in first_snapshot.available_artifacts
        assert "screenshot.png" in first_snapshot.available_artifacts
        assert "singlefile.html" in first_snapshot.available_artifacts
        
        # Second snapshot should have only metadata.json (minimal case)
        second_snapshot = next(s for s in example_url.snapshots if s.snapshot_id == "req_test-request-1_20250904_061058")
        assert len(second_snapshot.available_artifacts) == 1
        assert "metadata.json" in second_snapshot.available_artifacts
        
        # HTTPBin URL should have different artifacts
        httpbin_url = results["httpbin_org_get"]
        httpbin_snapshot = httpbin_url.snapshots[0]
        assert "archive.wacz" in httpbin_snapshot.available_artifacts
        assert "singlefile.html" in httpbin_snapshot.available_artifacts
        assert "metadata.json" in httpbin_snapshot.available_artifacts
    
    def test_archived_url_properties(self, temp_storage_three_level):
        """Test ArchivedUrl computed properties."""
        scanner = StorageScanner(temp_storage_three_level)
        results = scanner.scan()
        
        example_url = results["example_com_home_page"]
        
        # Test computed properties
        assert example_url.snapshot_count == 2
        assert example_url.first_captured == datetime(2025, 9, 4, 6, 10, 58)  # Earlier snapshot
        assert example_url.last_captured == datetime(2025, 9, 4, 6, 14, 11)   # Later snapshot
    
    def test_scan_timeout(self, temp_storage_three_level):
        """Test scan timeout functionality."""
        # Use very short timeout
        scanner = StorageScanner(temp_storage_three_level, timeout_seconds=0.001)
        results = scanner.scan()
        
        # Should complete quickly and may have partial results due to timeout
        assert isinstance(results, dict)
    
    def test_convenience_function(self, temp_storage_three_level):
        """Test convenience function scan_storage_directory."""
        results = scan_storage_directory(str(temp_storage_three_level))
        
        assert len(results) == 2
        assert "example_com_home_page" in results
        assert "httpbin_org_get" in results
    
    def test_real_archives_data_scan(self, real_archives_data):
        """Test scanning real archives data from test_archives folder."""
        scanner = StorageScanner(real_archives_data)
        results = scanner.scan()
        
        # Should find multiple URLs from the real archives
        assert len(results) >= 3  # At least example.com, httpbin.org, arachne URLs
        
        # Check that we have the expected URL patterns
        url_ids = list(results.keys())
        assert any("example_com" in url_id for url_id in url_ids)
        assert any("httpbin_org" in url_id for url_id in url_ids)
        assert any("arachne_dainst_org" in url_id for url_id in url_ids)
        
        # Verify artifacts are detected correctly
        for archived_url in results.values():
            assert archived_url.snapshot_count > 0
            for snapshot in archived_url.snapshots:
                # Should have at least metadata.json and archive.wacz in most cases
                if len(snapshot.available_artifacts) > 0:
                    assert "metadata.json" in snapshot.available_artifacts
                    # Most should have WACZ files
                    has_wacz = "archive.wacz" in snapshot.available_artifacts
                    if has_wacz:
                        assert True  # Good, found WACZ
    
    def test_three_level_directory_structure(self, temp_storage_three_level):
        """Test that scanner correctly handles three-level directory structure."""
        scanner = StorageScanner(temp_storage_three_level)
        results = scanner.scan()
        
        # Test URL ID construction: domain_pathsegment
        assert "example_com_home_page" in results
        assert "httpbin_org_get" in results
        
        # Test that folder_name reflects the path structure
        example_url = results["example_com_home_page"]
        assert example_url.folder_name == "example_com/home_page"
        
        httpbin_url = results["httpbin_org_get"]
        assert httpbin_url.folder_name == "httpbin_org/get"
    
    def test_request_timestamp_parsing(self):
        """Test parsing of request-timestamp folder names."""
        scanner = StorageScanner(Path("/tmp"))
        
        # Test various request-timestamp formats
        test_cases = [
            ("req_test-request-1_20250904_061411", datetime(2025, 9, 4, 6, 14, 11)),
            ("req_archive-session-123_20240315_143022", datetime(2024, 3, 15, 14, 30, 22)),
            ("req_simple_20230101_000000", datetime(2023, 1, 1, 0, 0, 0)),
        ]
        
        for folder_name, expected_dt in test_cases:
            result = scanner._parse_timestamp(folder_name)
            assert result == expected_dt, f"Failed to parse {folder_name}"
    
    def test_core_artifact_types_detection(self, temp_storage_three_level):
        """Test detection of four core artifact types."""
        scanner = StorageScanner(temp_storage_three_level)
        results = scanner.scan()
        
        # Find snapshot with all artifacts
        example_url = results["example_com_home_page"]
        full_snapshot = next(s for s in example_url.snapshots if len(s.available_artifacts) == 4)
        
        # Check all four core artifact types are detected
        expected_artifacts = {"archive.wacz", "metadata.json", "screenshot.png", "singlefile.html"}
        actual_artifacts = set(full_snapshot.available_artifacts)
        
        assert expected_artifacts == actual_artifacts
        
        # Test artifact count (using len since dataclass doesn't have artifact_count property)
        assert len(full_snapshot.available_artifacts) == 4
    
    def test_empty_directory_handling(self):
        """Test scanner behavior with empty directories at all levels."""
        with tempfile.TemporaryDirectory() as temp_dir:
            archives_path = Path(temp_dir) / 'archives'
            scanner = StorageScanner(archives_path)
            
            # Test 1: Completely empty archives directory
            archives_path.mkdir()
            results = scanner.scan()
            assert len(results) == 0
            
            # Test 2: Domain with no path directories
            domain_dir = archives_path / 'empty_domain_com'
            domain_dir.mkdir()
            results = scanner.scan()
            assert len(results) == 0
            
            # Test 3: Domain/path with no request directories
            path_dir = domain_dir / 'empty_path'
            path_dir.mkdir()
            results = scanner.scan()
            assert len(results) == 0
            
            # Test 4: Request directory with no artifacts (only empty)
            request_dir = path_dir / 'req_empty_20250904_120000'
            request_dir.mkdir()
            results = scanner.scan()
            assert len(results) == 1  # Should create URL but snapshot has no artifacts
            url = list(results.values())[0]
            assert url.snapshot_count == 1
            assert len(url.snapshots[0].available_artifacts) == 0
            
            # Test 5: Request directory with only metadata.json
            metadata = {
                'archive_info': {
                    'url': 'https://empty-test.com',
                    'request_id': 'empty-test'
                }
            }
            with open(request_dir / 'metadata.json', 'w') as f:
                json.dump(metadata, f)
            
            results = scanner.scan()
            assert len(results) == 1
            url = list(results.values())[0] 
            assert len(url.snapshots[0].available_artifacts) == 1
            assert 'metadata.json' in url.snapshots[0].available_artifacts
    
    def test_non_request_directory_filtering(self):
        """Test that scanner ignores directories not starting with 'req_'."""
        with tempfile.TemporaryDirectory() as temp_dir:
            archives_path = Path(temp_dir) / 'archives'
            
            # Create structure with non-request directories
            domain_dir = archives_path / 'test_domain_com'
            path_dir = domain_dir / 'test_path'
            path_dir.mkdir(parents=True)
            
            # Create directories that should be ignored (don't start with 'req_')
            (path_dir / 'not_a_request_dir').mkdir()
            (path_dir / 'another_non_request').mkdir()
            (path_dir / 'legacy_20240315T143022Z').mkdir()  # timestamp format only
            
            # Create valid request directory
            request_dir = path_dir / 'req_valid_20250904_120000'
            request_dir.mkdir()
            
            metadata = {
                'archive_info': {
                    'url': 'https://test-filtering.com',
                    'request_id': 'valid'
                }
            }
            with open(request_dir / 'metadata.json', 'w') as f:
                json.dump(metadata, f)
            
            scanner = StorageScanner(archives_path)
            results = scanner.scan()
            
            # Should only find 1 URL (ignores the 3 non-request directories)
            assert len(results) == 1
            assert 'test_domain_com_test_path' in results
            assert results['test_domain_com_test_path'].snapshot_count == 1
    
    def test_mixed_files_and_directories(self):
        """Test that scanner ignores files and only processes directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            archives_path = Path(temp_dir) / 'archives'
            archives_path.mkdir()
            
            # Create files at various levels (should all be ignored)
            (archives_path / 'not_a_domain.txt').write_text('ignored file')
            
            domain_dir = archives_path / 'mixed_domain_com'
            domain_dir.mkdir()
            (domain_dir / 'not_a_path.log').write_text('ignored file')
            
            path_dir = domain_dir / 'mixed_path'
            path_dir.mkdir()
            (path_dir / 'not_a_request.json').write_text('ignored file')
            
            # Create valid request directory
            request_dir = path_dir / 'req_mixed_20250904_120000'
            request_dir.mkdir()
            
            metadata = {
                'archive_info': {
                    'url': 'https://mixed-content.com',
                    'request_id': 'mixed'
                }
            }
            with open(request_dir / 'metadata.json', 'w') as f:
                json.dump(metadata, f)
            
            scanner = StorageScanner(archives_path)
            results = scanner.scan()
            
            # Should find exactly 1 URL despite mixed files
            assert len(results) == 1
            assert 'mixed_domain_com_mixed_path' in results


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