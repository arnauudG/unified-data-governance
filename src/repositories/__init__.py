"""
Repository Pattern implementation for data access abstraction.

This package provides repositories for accessing external services:
- Soda Cloud API
- Collibra API
"""

from src.repositories.base import BaseRepository
from src.repositories.soda_repository import SodaRepository
from src.repositories.collibra_repository import CollibraRepository

__all__ = [
    "BaseRepository",
    "SodaRepository",
    "CollibraRepository",
]
