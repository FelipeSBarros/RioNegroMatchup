"""
api.py
======
Single-import surface for the aquamatch pipeline wrappers.

All three pipeline mode are importable from here so callers do not need
to know which module each function lives in::

    from aquamatch.api import (
        run_insitu_pipeline,
        run_sentinel_pipeline,
        run_acolite_pipeline,
    )

The functions are defined in their home modules and re-exported here:

* :func:`run_insitu_pipeline`   — :mod:`aquamatch.insitu_data`
* :func:`run_sentinel_pipeline` — :mod:`aquamatch.sentinel_data`
* :func:`run_acolite_pipeline`  — :mod:`aquamatch.acolite_spec`

Each wrapper accepts ``None`` for all path arguments (falling back to
project defaults) and returns a consistent status dict::

    {
        "step":             str,    # "insitu" | "sentinel" | "acolite"
        "status":           str,    # "success" | "error"
        "outputs":          dict,   # step-specific results
        "error":            str | None,
        "elapsed_seconds":  float,
    }

Example — full pipeline in a script::

    from aquamatch.api import (
        run_insitu_pipeline,
        run_sentinel_pipeline,
        run_acolite_pipeline,
    )

    r1 = run_insitu_pipeline(
        stations="data/original_data/my_stations.xlsx",
        campaigns="data/original_data/my_export.xlsx",
    )

    if r1["status"] == "success":
        r2 = run_sentinel_pipeline(
            unique_csv=r1["outputs"]["unique_csv"],
            time_delta=2,
            cloud_cover=20,
        )

    if r2["status"] == "success":
        r3 = run_acolite_pipeline(
            acolite_executable="/path/to/acolite/acolite.py",
            safe_dir=r2["outputs"]["output_dir"],
            use_scl=True,
        )
"""

from aquamatch.insitu_data import run_insitu_pipeline
from aquamatch.sentinel_data import run_sentinel_pipeline
from aquamatch.acolite_spec import run_acolite_pipeline

__all__ = [
    "run_insitu_pipeline",
    "run_sentinel_pipeline",
    "run_acolite_pipeline",
]
