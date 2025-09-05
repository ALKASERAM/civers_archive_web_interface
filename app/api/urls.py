"""
URLs API endpoint implementation.

This module provides the GET /api/urls endpoint with pagination and sorting.
"""

import logging
import math
from datetime import datetime
from enum import Enum

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from ..models.url import ArchivedUrl
from ..models.responses import PaginatedResponse, PaginationMeta, ErrorResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["URLs"])

class SortOption(str, Enum):
    """Available sorting options for URL list."""
    URL = "url"
    LAST_CAPTURED = "last_captured"
    SNAPSHOT_COUNT = "snapshot_count"


class UrlListSummary(BaseModel):
    """Summary model for URL list responses (without full snapshots)."""
    url_id: str
    original_url: str
    folder_name: str
    snapshot_count: int
    first_captured: str | None = None
    last_captured: str | None = None
    date_range: str | None = None

    @classmethod
    def from_archived_url(cls, archived_url: ArchivedUrl) -> 'UrlListSummary':
        """Convert ArchivedUrl to summary format."""
        return cls(
            url_id=archived_url.url_id,
            original_url=str(archived_url.original_url),
            folder_name=archived_url.folder_name,
            snapshot_count=archived_url.snapshot_count,
            first_captured=archived_url.first_captured.isoformat() if archived_url.first_captured else None,
            last_captured=archived_url.last_captured.isoformat() if archived_url.last_captured else None,
            date_range=archived_url.date_range
        )

@router.get(
    "/urls",
    response_model=PaginatedResponse[UrlListSummary],
    responses={
        400: {"model": ErrorResponse, "description": "Invalid query parameters"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    },
    summary="List all archived URLs",
    description="Retrieve a paginated list of all archived URLs with sorting options"
)
async def list_urls(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    limit: int = Query(50, ge=1, le=100, description="Number of items per page (1-100)"),
    sort: SortOption = Query(SortOption.URL, description="Sort order for results")
):
    """
    Get paginated list of archived URLs with sorting.
    
    Args:
        page: Page number (1-based, default: 1)
        limit: Items per page (1-100, default: 50)  
        sort: Sort option (url, last_captured, snapshot_count, default: url)
        
    Returns:
        PaginatedResponse containing URL summaries and pagination metadata
        
    Raises:
        HTTPException: For invalid parameters or server errors
    """
    try:
        # Get storage service from app state
        storage_service = request.app.state.storage_service
        logger.info(f"Fetching URLs - page: {page}, limit: {limit}, sort: {sort}")
        
        # Get all URLs from storage service (with caching)
        url_dict = storage_service.get_all_urls()
        
        if not url_dict:
            logger.warning("No URLs found in storage")
            return PaginatedResponse[UrlListSummary](
                success=True,
                data=[],
                pagination=PaginationMeta.create(page=page, limit=limit, total_count=0)
            )
        
        # Convert to list of ArchivedUrl objects
        archived_urls = list(url_dict.values())
        
        # Apply sorting
        if sort == SortOption.URL:
            archived_urls.sort(key=lambda u: str(u.original_url).lower())
        elif sort == SortOption.LAST_CAPTURED:
            archived_urls.sort(key=lambda u: u.last_captured or datetime.min, reverse=True)
        elif sort == SortOption.SNAPSHOT_COUNT:
            archived_urls.sort(key=lambda u: u.snapshot_count, reverse=True)
        
        # Calculate pagination
        total_count = len(archived_urls)
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        
        # Validate page bounds
        if page > 1 and start_idx >= total_count:
            raise HTTPException(
                status_code=400,
                detail=f"Page {page} does not exist. Total pages: {math.ceil(total_count / limit)}"
            )
        
        # Get page slice
        page_urls = archived_urls[start_idx:end_idx]
        
        # Convert to summary format
        url_summaries = [UrlListSummary.from_archived_url(url) for url in page_urls]
        
        # Create pagination metadata
        pagination = PaginationMeta.create(page=page, limit=limit, total_count=total_count)
        
        logger.info(f"Returning {len(url_summaries)} URLs (page {page}/{pagination.total_pages})")
        
        return PaginatedResponse[UrlListSummary](
            success=True,
            data=url_summaries,
            pagination=pagination
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching URL list: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve URL list: {str(e)}"
        )


