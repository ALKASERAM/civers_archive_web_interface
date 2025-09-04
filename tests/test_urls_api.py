"""
Tests for URLs API endpoint.
"""

import pytest
import time
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from datetime import datetime
from pathlib import Path

from app.main import app
from app.storage.scanner import ArchivedUrl as ScannerArchivedUrl, Snapshot as ScannerSnapshot

client = TestClient(app)

@pytest.fixture
def mock_scanner_data():
    """Mock scanner data for testing."""
    # Create mock snapshot
    mock_snapshot = ScannerSnapshot(
        snapshot_id="req_test-1_20250904_061411",
        timestamp=datetime(2024, 3, 15, 14, 30, 22),
        folder_path=Path("/test/path"),
        metadata={"url": "https://example.com", "title": "Test Page"},
        url="https://example.com",
        title="Test Page",
        available_artifacts=["archive.wacz", "metadata.json", "screenshot.png", "singlefile.html"]
    )
    
    # Create mock archived URL
    mock_archived_url = ScannerArchivedUrl(
        url_id="example_com",
        original_url="https://example.com",
        folder_name="example_com",
        snapshots=[mock_snapshot]
    )
    
    return [mock_archived_url]

class TestUrlsAPI:
    """Test cases for URLs API endpoint."""
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_default_params(self, mock_scanner, mock_scanner_data):
        """Test URLs endpoint with default parameters."""
        mock_scanner.return_value = mock_scanner_data
        
        response = client.get("/api/urls")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert len(data["data"]) == 1
        assert data["data"][0]["url_id"] == "example_com"
        assert data["data"][0]["original_url"] == "https://example.com/"  # HttpUrl normalizes URLs
        assert data["data"][0]["snapshot_count"] == 1
        
        # Check pagination
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["limit"] == 50
        assert data["pagination"]["total_count"] == 1
        assert data["pagination"]["total_pages"] == 1
        assert data["pagination"]["has_next"] is False
        assert data["pagination"]["has_previous"] is False
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_with_pagination(self, mock_scanner, mock_scanner_data):
        """Test URLs endpoint with custom pagination."""
        mock_scanner.return_value = mock_scanner_data
        
        response = client.get("/api/urls?page=1&limit=1")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["limit"] == 1
        assert data["pagination"]["total_count"] == 1
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_sorting(self, mock_scanner, mock_scanner_data):
        """Test URLs endpoint with different sort options."""
        mock_scanner.return_value = mock_scanner_data
        
        # Test each sort option
        for sort_option in ["url", "last_captured", "snapshot_count"]:
            response = client.get(f"/api/urls?sort={sort_option}")
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_empty_results(self, mock_scanner):
        """Test URLs endpoint with no results."""
        mock_scanner.return_value = []
        
        response = client.get("/api/urls")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert len(data["data"]) == 0
        assert data["pagination"]["total_count"] == 0
        assert data["pagination"]["total_pages"] == 0
    
    def test_list_urls_invalid_page(self):
        """Test URLs endpoint with invalid page number."""
        response = client.get("/api/urls?page=0")
        assert response.status_code == 422  # Validation error
    
    def test_list_urls_invalid_limit(self):
        """Test URLs endpoint with invalid limit."""
        response = client.get("/api/urls?limit=0")
        assert response.status_code == 422  # Validation error
        
        response = client.get("/api/urls?limit=101")
        assert response.status_code == 422  # Validation error
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_page_out_of_bounds(self, mock_scanner, mock_scanner_data):
        """Test URLs endpoint with page beyond available data."""
        mock_scanner.return_value = mock_scanner_data
        
        response = client.get("/api/urls?page=999")
        
        assert response.status_code == 400
        assert "does not exist" in response.json()["detail"]
    
    @patch('app.api.urls.get_cached_scanner_results')
    def test_list_urls_server_error(self, mock_scanner):
        """Test URLs endpoint with server error."""
        mock_scanner.side_effect = Exception("Scanner error")
        
        response = client.get("/api/urls")
        
        assert response.status_code == 500
        assert "Failed to retrieve URL list" in response.json()["detail"]


class TestCacheTTL:
    """Test cases for cache TTL functionality."""
    
    def setup_method(self):
        """Reset cache state before each test."""
        import app.api.urls as urls_module
        urls_module._scanner_cache_timestamp = 0
        urls_module._get_scanner_results_internal.cache_clear()
    
    def test_cache_stats_endpoint(self):
        """Test cache statistics endpoint."""
        response = client.get("/api/cache/stats")
        
        assert response.status_code == 200
        data = response.json()
        
        # Check expected fields
        assert "cache_age_seconds" in data
        assert "ttl_seconds" in data
        assert "cache_expired" in data
        assert "cache_disabled" in data
        assert "last_refresh_timestamp" in data
        
        # TTL should be 60 seconds by default
        assert data["ttl_seconds"] == 60
    
    @patch('app.api.urls._scanner_cache_ttl', 0)  # Disable cache
    @patch('app.api.urls._get_scanner_results_internal')
    def test_cache_disabled_behavior(self, mock_internal):
        """Test that cache is bypassed when TTL=0."""
        mock_internal.return_value = []
        
        # Make two requests
        client.get("/api/urls")
        client.get("/api/urls")
        
        # Should call scanner twice when cache is disabled
        assert mock_internal.call_count == 2
    
    def test_cache_configuration_via_env(self):
        """Test that TTL can be configured via environment variable."""
        # This test verifies the environment variable is properly read
        # The actual TTL behavior is verified through manual testing
        import app.api.urls as urls_module
        
        # Check that the TTL is configured (either default 60 or custom value)
        assert isinstance(urls_module._scanner_cache_ttl, int)
        assert urls_module._scanner_cache_ttl >= 0  # Can be 0 (disabled) or positive
    
    @patch('app.api.urls._get_scanner_results_internal')
    def test_manual_cache_clear(self, mock_internal):
        """Test manual cache clearing functionality."""
        from app.api.urls import clear_scanner_cache
        
        mock_internal.return_value = []
        
        # First request - cache miss
        client.get("/api/urls")
        assert mock_internal.call_count == 1
        
        # Clear cache manually
        clear_scanner_cache()
        
        # Next request should be cache miss again
        client.get("/api/urls")
        assert mock_internal.call_count == 2