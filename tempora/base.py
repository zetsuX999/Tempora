"""
Tempora Base Models
===================

Base model classes for Tempora entities providing standard fields
and behaviors for all scheduler models.

This is a standalone base - no external dependencies required.
"""

from django.db import models


class TemporaEntity(models.Model):
    """
    Abstract base class for all Tempora models.

    Provides:
    - Abstract model status (no table created)
    - Standard updated_at field for change tracking
    - Extensible Meta class for inheritance

    All Tempora models should inherit from this class.
    """

    updated_at = models.DateTimeField(
        auto_now=True,
        help_text="Last modification timestamp",
    )

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        """Standard save with optional pre-save hooks."""
        super().save(*args, **kwargs)
