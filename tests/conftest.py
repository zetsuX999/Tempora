"""
Pytest configuration for Tempora tests.
"""

import os
import django
from django.conf import settings

# Configure Django settings before importing models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tempora.settings")

# Ensure Django is configured
if not settings.configured:
    django.setup()

import pytest


@pytest.fixture(scope="session")
def django_db_setup():
    """Configure Django database for testing."""
    pass


@pytest.fixture
def mock_cluster_secret():
    """Provide a mock cluster secret for testing."""
    return "test-cluster-secret-32-bytes-min"
