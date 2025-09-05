"""
Storage providers package for the Civers Archive Web Interface.

This package contains the storage provider implementations for different
storage backends (filesystem, S3, database, etc.).
"""

from .base import StorageProvider
from .filesystem import FilesystemStorageProvider

__all__ = [
    "StorageProvider",
    "FilesystemStorageProvider",
]