"""
Minimal Django settings for Tempora standalone operation.

Configure via environment variables:
- TEMPORA_SECRET_KEY: Django secret key
- TEMPORA_DEBUG: Enable debug mode (default: False)
- TEMPORA_DB_NAME: PostgreSQL database name
- TEMPORA_DB_USER: PostgreSQL user
- TEMPORA_DB_PASSWORD: PostgreSQL password
- TEMPORA_DB_HOST: PostgreSQL host (default: localhost)
- TEMPORA_DB_PORT: PostgreSQL port (default: 5432)
"""

import os
from pathlib import Path

# Build paths inside the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Security
SECRET_KEY = os.environ.get(
    "TEMPORA_SECRET_KEY",
    "dev-secret-key-for-testing-only-change-in-production"
)
DEBUG = os.environ.get("TEMPORA_DEBUG", "False").lower() in ("true", "1", "yes")
ALLOWED_HOSTS = os.environ.get("TEMPORA_ALLOWED_HOSTS", "*").split(",")

# Application definition
INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "tempora",
]

# Database
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("TEMPORA_DB_NAME", "tempora"),
        "USER": os.environ.get("TEMPORA_DB_USER", "tempora"),
        "PASSWORD": os.environ.get("TEMPORA_DB_PASSWORD", ""),
        "HOST": os.environ.get("TEMPORA_DB_HOST", "localhost"),
        "PORT": os.environ.get("TEMPORA_DB_PORT", "5432"),
    }
}

# Default primary key field type
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Internationalization
USE_TZ = True
TIME_ZONE = "UTC"

# Logging
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
    "loggers": {
        "tempora": {
            "handlers": ["console"],
            "level": os.environ.get("TEMPORA_LOG_LEVEL", "INFO"),
            "propagate": False,
        },
    },
}
