"""
aquamatch
=========
Python package for matching Sentinel-2 satellite imagery with in situ
water quality measurements, applying atmospheric correction, and
validating remote sensing water quality products.

The three pipeline mode are available directly from the package::

    from aquamatch import (
        run_insitu_pipeline,
        run_sentinel_pipeline,
        run_acolite_pipeline,
    )

They are also importable from :mod:`aquamatch.api` for callers who prefer
an explicit import path::

    from aquamatch.api import run_insitu_pipeline
"""

from aquamatch.api import (
    run_insitu_pipeline,
    run_sentinel_pipeline,
    run_acolite_pipeline,
)

__all__ = [
    "run_insitu_pipeline",
    "run_sentinel_pipeline",
    "run_acolite_pipeline",
]
