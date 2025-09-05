"""
Storage factory for creating storage providers and services.

This module provides factory functions for creating storage providers
and services based on configuration.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

from .providers.base import StorageProvider
from .providers.filesystem import FilesystemStorageProvider
from .service import StorageService

logger = logging.getLogger(__name__)


class StorageConfigurationError(Exception):
    """Exception raised for storage configuration errors."""
    pass


def load_storage_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load storage configuration from YAML file with environment variable overrides.
    
    Args:
        config_path: Path to configuration file (default: config/storage.yaml)
        
    Returns:
        Configuration dictionary
        
    Raises:
        StorageConfigurationError: If configuration loading fails
    """
    try:
        # Use default config path if not specified
        if config_path is None:
            config_path = Path("config/storage.yaml")
        
        # Load YAML configuration
        if not config_path.exists():
            raise StorageConfigurationError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config or 'storage' not in config:
            raise StorageConfigurationError("Invalid configuration: missing 'storage' section")
        
        # Apply environment variable overrides
        config = _apply_environment_overrides(config)
        
        return config
        
    except yaml.YAMLError as e:
        raise StorageConfigurationError(f"YAML parsing error: {e}") from e
    except Exception as e:
        raise StorageConfigurationError(f"Configuration loading failed: {e}") from e


def _apply_environment_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply environment variable overrides to configuration.
    
    Environment variables follow the pattern: CIVERS_<SECTION>_<KEY>
    
    Args:
        config: Base configuration dictionary
        
    Returns:
        Configuration with environment overrides applied
    """
    # Storage type override
    storage_type = os.getenv('CIVERS_STORAGE_TYPE')
    if storage_type:
        config['storage']['type'] = storage_type
        logger.info(f"Storage type overridden by environment: {storage_type}")
    
    # Filesystem overrides
    fs_path = os.getenv('CIVERS_FILESYSTEM_PATH')
    if fs_path:
        if 'filesystem' not in config['storage']:
            config['storage']['filesystem'] = {}
        config['storage']['filesystem']['path'] = fs_path
        logger.info(f"Filesystem path overridden by environment: {fs_path}")
    
    fs_timeout = os.getenv('CIVERS_FILESYSTEM_TIMEOUT_SECONDS')
    if fs_timeout:
        if 'filesystem' not in config['storage']:
            config['storage']['filesystem'] = {}
        config['storage']['filesystem']['timeout_seconds'] = int(fs_timeout)
        logger.info(f"Filesystem timeout overridden by environment: {fs_timeout}")
    
    # Cache overrides
    cache_ttl = os.getenv('CIVERS_CACHE_TTL_SECONDS')
    if cache_ttl:
        if 'cache' not in config['storage']:
            config['storage']['cache'] = {}
        config['storage']['cache']['ttl_seconds'] = int(cache_ttl)
        logger.info(f"Cache TTL overridden by environment: {cache_ttl}")
    
    # Legacy environment variable support (for backward compatibility)
    legacy_ttl = os.getenv('SCANNER_CACHE_TTL')
    if legacy_ttl and not cache_ttl:
        if 'cache' not in config['storage']:
            config['storage']['cache'] = {}
        config['storage']['cache']['ttl_seconds'] = int(legacy_ttl)
        logger.info(f"Cache TTL set from legacy environment variable: {legacy_ttl}")
    
    return config


def create_storage_provider(config: Dict[str, Any]) -> StorageProvider:
    """
    Create storage provider based on configuration.
    
    Args:
        config: Storage configuration dictionary
        
    Returns:
        StorageProvider instance
        
    Raises:
        StorageConfigurationError: If provider creation fails
    """
    try:
        storage_config = config['storage']
        provider_type = storage_config.get('type', 'filesystem')
        
        if provider_type == 'filesystem':
            return _create_filesystem_provider(storage_config)
        elif provider_type == 's3':
            raise StorageConfigurationError("S3 storage provider not yet implemented")
        elif provider_type == 'database':
            raise StorageConfigurationError("Database storage provider not yet implemented")
        else:
            raise StorageConfigurationError(f"Unknown storage provider type: {provider_type}")
            
    except KeyError as e:
        raise StorageConfigurationError(f"Missing required configuration key: {e}") from e
    except Exception as e:
        raise StorageConfigurationError(f"Provider creation failed: {e}") from e


def _create_filesystem_provider(storage_config: Dict[str, Any]) -> FilesystemStorageProvider:
    """
    Create filesystem storage provider.
    
    Args:
        storage_config: Storage section of configuration
        
    Returns:
        FilesystemStorageProvider instance
    """
    fs_config = storage_config.get('filesystem', {})
    
    # Get storage path
    storage_path = Path(fs_config.get('path', 'archives'))
    if not storage_path.is_absolute():
        # Make relative paths relative to project root
        storage_path = Path.cwd() / storage_path
    
    # Get timeout
    timeout_seconds = fs_config.get('timeout_seconds', 10)
    
    logger.info(f"Creating filesystem storage provider: path={storage_path}, timeout={timeout_seconds}s")
    
    return FilesystemStorageProvider(storage_path, timeout_seconds)


def create_storage_service(config: Dict[str, Any], provider: Optional[StorageProvider] = None) -> StorageService:
    """
    Create storage service with provider and caching.
    
    Args:
        config: Storage configuration dictionary
        provider: Optional storage provider (will be created if not provided)
        
    Returns:
        StorageService instance
        
    Raises:
        StorageConfigurationError: If service creation fails
    """
    try:
        # Create provider if not provided
        if provider is None:
            provider = create_storage_provider(config)
        
        # Get cache configuration
        cache_config = config['storage'].get('cache', {})
        cache_ttl = cache_config.get('ttl_seconds', 60)
        
        logger.info(f"Creating storage service with cache TTL: {cache_ttl}s")
        
        return StorageService(provider, cache_ttl)
        
    except Exception as e:
        raise StorageConfigurationError(f"Service creation failed: {e}") from e


def create_default_storage_service(config_path: Optional[Path] = None) -> StorageService:
    """
    Create storage service with default configuration.
    
    This is the main entry point for creating a storage service with
    configuration loaded from file and environment overrides.
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        StorageService instance ready for use
        
    Raises:
        StorageConfigurationError: If configuration or creation fails
    """
    try:
        # Load configuration
        config = load_storage_config(config_path)
        
        # Create and return service
        return create_storage_service(config)
        
    except Exception as e:
        logger.error(f"Failed to create default storage service: {e}")
        raise StorageConfigurationError(f"Default service creation failed: {e}") from e