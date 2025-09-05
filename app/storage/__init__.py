"""
Storage module for Civers Archive Web Interface

This module provides the extensible storage architecture with providers,
service layer, and configuration system.
"""

from .service import StorageService
from .factory import create_default_storage_service, create_storage_service, create_storage_provider
from .providers.base import StorageProvider
from .providers.filesystem import FilesystemStorageProvider

__all__ = [
    "StorageService",
    "create_default_storage_service", 
    "create_storage_service",
    "create_storage_provider",
    "StorageProvider",
    "FilesystemStorageProvider"
]