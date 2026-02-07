"""
Connection pooling utilities for the unified data governance platform.

This module provides connection pooling functionality to optimize
database and API connection reuse.
"""

from typing import Optional, Dict, Any, Generic, TypeVar
from contextlib import contextmanager
from threading import Lock
from queue import Queue, Empty
import time

from src.core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class ConnectionPool(Generic[T]):
    """
    Generic connection pool for managing reusable connections.
    
    Provides connection pooling to reduce connection overhead
    and improve performance.
    """

    def __init__(
        self,
        factory: Any,
        max_size: int = 10,
        min_size: int = 2,
        timeout: float = 30.0,
    ):
        """
        Initialize connection pool.

        Args:
            factory: Callable that creates a new connection
            max_size: Maximum pool size
            min_size: Minimum pool size (connections created on init)
            timeout: Timeout for getting connection from pool
        """
        self.factory = factory
        self.max_size = max_size
        self.min_size = min_size
        self.timeout = timeout
        self._pool: Queue[T] = Queue(maxsize=max_size)
        self._lock = Lock()
        self._created = 0
        self._active = 0

        # Create initial connections
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize pool with minimum connections."""
        for _ in range(self.min_size):
            try:
                conn = self.factory()
                self._pool.put(conn, block=False)
                self._created += 1
            except Exception as e:
                logger.warning(f"Failed to create initial connection: {e}")

        logger.info(f"Initialized connection pool with {self._pool.qsize()} connections")

    def _create_connection(self) -> T:
        """
        Create a new connection.

        Returns:
            New connection instance
        """
        logger.debug("Creating new connection")
        conn = self.factory()
        self._created += 1
        return conn

    def get(self) -> T:
        """
        Get a connection from the pool.

        Returns:
            Connection instance

        Raises:
            TimeoutError: If timeout exceeded while waiting for connection
        """
        try:
            # Try to get from pool
            conn = self._pool.get(timeout=self.timeout)
            self._active += 1
            logger.debug(f"Got connection from pool (active: {self._active}, pool: {self._pool.qsize()})")
            return conn
        except Empty:
            # Pool empty, check if we can create more
            with self._lock:
                if self._created < self.max_size:
                    conn = self._create_connection()
                    self._active += 1
                    logger.debug(f"Created new connection (active: {self._active}, created: {self._created})")
                    return conn
                else:
                    raise TimeoutError(
                        f"Connection pool exhausted (max_size={self.max_size})"
                    )

    def put(self, conn: T) -> None:
        """
        Return a connection to the pool.

        Args:
            conn: Connection to return
        """
        try:
            self._pool.put(conn, block=False)
            self._active -= 1
            logger.debug(f"Returned connection to pool (active: {self._active}, pool: {self._pool.qsize()})")
        except Exception as e:
            # Pool full or connection invalid, discard it
            logger.warning(f"Failed to return connection to pool: {e}")
            self._active -= 1
            try:
                # Try to close/dispose connection
                if hasattr(conn, "close"):
                    conn.close()
            except Exception:
                pass

    @contextmanager
    def connection(self):
        """
        Context manager for getting and returning connections.

        Example:
            with pool.connection() as conn:
                # Use connection
                pass
        """
        conn = self.get()
        try:
            yield conn
        finally:
            self.put(conn)

    def close_all(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            closed_count = 0
            while not self._pool.empty():
                try:
                    conn = self._pool.get(block=False)
                    if hasattr(conn, "close"):
                        conn.close()
                    closed_count += 1
                except Empty:
                    break

            self._pool = Queue(maxsize=self.max_size)
            self._created = 0
            self._active = 0
            logger.info(f"Closed {closed_count} connections in pool")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get pool statistics.

        Returns:
            Dictionary with pool statistics
        """
        return {
            "max_size": self.max_size,
            "min_size": self.min_size,
            "created": self._created,
            "active": self._active,
            "available": self._pool.qsize(),
            "utilization": self._active / self.max_size if self.max_size > 0 else 0,
        }


class SessionPool:
    """
    HTTP session pool for API connections.
    
    Optimizes HTTP session reuse for API calls.
    """

    def __init__(self, max_size: int = 10):
        """
        Initialize session pool.

        Args:
            max_size: Maximum pool size
        """
        self.max_size = max_size
        self._sessions: Queue = Queue(maxsize=max_size)
        self._lock = Lock()

    def get_session(self, factory: Any):
        """
        Get or create a session.

        Args:
            factory: Callable that creates a new session

        Returns:
            Session instance
        """
        try:
            return self._sessions.get(block=False)
        except Empty:
            return factory()

    def return_session(self, session: Any) -> None:
        """
        Return session to pool.

        Args:
            session: Session to return
        """
        try:
            self._sessions.put(session, block=False)
        except Exception:
            # Pool full, discard session
            if hasattr(session, "close"):
                session.close()

    def close_all(self) -> None:
        """Close all sessions in pool."""
        while not self._sessions.empty():
            try:
                session = self._sessions.get(block=False)
                if hasattr(session, "close"):
                    session.close()
            except Empty:
                break
