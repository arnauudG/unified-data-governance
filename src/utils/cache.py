"""
Caching utilities for the unified data governance platform.

This module provides caching functionality with TTL (time-to-live)
support for API responses and expensive computations.
"""

import time
from typing import Any, Optional, Dict, Callable, TypeVar
from functools import wraps
from datetime import datetime, timedelta
from threading import Lock

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class TTLCache:
    """
    Thread-safe TTL (Time-To-Live) cache.
    
    Provides caching with expiration support for storing
    API responses and computed values.
    """

    def __init__(self, default_ttl: int = 300):
        """
        Initialize TTL cache.

        Args:
            default_ttl: Default TTL in seconds (default: 5 minutes)
        """
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if not expired.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or expired
        """
        with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]
            expires_at = entry["expires_at"]

            if datetime.utcnow() > expires_at:
                # Entry expired, remove it
                del self._cache[key]
                logger.debug(f"Cache entry expired: {key}")
                return None

            logger.debug(f"Cache hit: {key}")
            return entry["value"]

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set value in cache with TTL.

        Args:
            key: Cache key
            value: Value to cache
            ttl: TTL in seconds (uses default if None)
        """
        with self._lock:
            ttl = ttl or self.default_ttl
            expires_at = datetime.utcnow() + timedelta(seconds=ttl)

            self._cache[key] = {
                "value": value,
                "expires_at": expires_at,
                "created_at": datetime.utcnow(),
            }

            logger.debug(f"Cached value: {key} (TTL: {ttl}s)")

    def delete(self, key: str) -> None:
        """
        Delete value from cache.

        Args:
            key: Cache key
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"Deleted cache entry: {key}")

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared {count} cache entries")

    def cleanup_expired(self) -> int:
        """
        Remove expired entries from cache.

        Returns:
            Number of entries removed
        """
        with self._lock:
            now = datetime.utcnow()
            expired_keys = [
                key
                for key, entry in self._cache.items()
                if now > entry["expires_at"]
            ]

            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

            return len(expired_keys)

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        with self._lock:
            now = datetime.utcnow()
            expired_count = sum(
                1
                for entry in self._cache.values()
                if now > entry["expires_at"]
            )

            return {
                "total_entries": len(self._cache),
                "expired_entries": expired_count,
                "active_entries": len(self._cache) - expired_count,
                "default_ttl": self.default_ttl,
            }


# Global cache instance
_global_cache: Optional[TTLCache] = None


def get_cache() -> TTLCache:
    """
    Get global cache instance (singleton).

    Returns:
        Global TTLCache instance
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = TTLCache()
    return _global_cache


def cached(ttl: Optional[int] = None, key_prefix: Optional[str] = None):
    """
    Decorator to cache function results.

    Args:
        ttl: TTL in seconds (uses cache default if None)
        key_prefix: Optional prefix for cache keys

    Example::

        @cached(ttl=300)
        def expensive_computation(arg1, arg2):
            return compute(arg1, arg2)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache = get_cache()

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Generate cache key
            key_parts = [key_prefix or func.__name__]
            key_parts.extend(str(arg) for arg in args)
            key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
            cache_key = ":".join(key_parts)

            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_value

            # Compute and cache
            logger.debug(f"Cache miss for {func.__name__}, computing...")
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl=ttl)

            return result

        return wrapper

    return decorator
