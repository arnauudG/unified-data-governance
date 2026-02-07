"""
Factory Pattern implementation for object creation.

This package provides factories for creating repositories, services, and clients
with proper dependency injection and configuration.
"""

from src.factories.client_factory import ClientFactory
from src.factories.service_factory import ServiceFactory

__all__ = [
    "ClientFactory",
    "ServiceFactory",
]
