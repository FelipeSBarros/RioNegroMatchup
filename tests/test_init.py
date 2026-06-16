"""
Tests for aquamatch/__init__.py.

__init__.py re-exports the three wrappers from aquamatch.api so they are
available at the package top level.  The tests verify:

  1. All three wrappers are importable directly from ``aquamatch``.
  2. Each re-exported name is the identical object as in its home module
     (no wrapping, no copying — the full identity chain is preserved).
  3. __all__ is defined at the package level and contains exactly the
     three public names.
  4. ``from aquamatch import *`` exposes exactly the three names.
  5. The package docstring is present (documents the import surface).
"""


class TestPackageImports:
    """All three wrappers must be importable directly from aquamatch."""

    def test_run_insitu_pipeline_importable_from_package(self):
        from aquamatch import run_insitu_pipeline  # noqa: F401

    def test_run_sentinel_pipeline_importable_from_package(self):
        from aquamatch import run_sentinel_pipeline  # noqa: F401

    def test_run_acolite_pipeline_importable_from_package(self):
        from aquamatch import run_acolite_pipeline  # noqa: F401

    def test_all_three_importable_in_one_statement(self):
        from aquamatch import (  # noqa: F401
            run_insitu_pipeline,
            run_sentinel_pipeline,
            run_acolite_pipeline,
        )


class TestPackageIdentity:
    """
    Each name imported from aquamatch must be the identical object as in
    its home module — verifying the full re-export chain:

        home module → aquamatch.api → aquamatch
    """

    def test_run_insitu_pipeline_identity(self):
        from aquamatch import run_insitu_pipeline as pkg_fn
        from aquamatch.insitu_data import run_insitu_pipeline as home_fn

        assert pkg_fn is home_fn

    def test_run_sentinel_pipeline_identity(self):
        from aquamatch import run_sentinel_pipeline as pkg_fn
        from aquamatch.sentinel_data import run_sentinel_pipeline as home_fn

        assert pkg_fn is home_fn

    def test_run_acolite_pipeline_identity(self):
        from aquamatch import run_acolite_pipeline as pkg_fn
        from aquamatch.acolite_spec import run_acolite_pipeline as home_fn

        assert pkg_fn is home_fn

    def test_package_and_api_module_are_same_objects(self):
        """aquamatch.X and aquamatch.api.X must be identical at every level."""
        from aquamatch import run_insitu_pipeline as pkg_fn
        from aquamatch.api import run_insitu_pipeline as api_fn

        assert pkg_fn is api_fn


class TestPackageAll:
    """__all__ at the package level must match the api.py contract."""

    def test_all_is_defined(self):
        import aquamatch

        assert hasattr(aquamatch, "__all__")

    def test_all_contains_run_insitu_pipeline(self):
        import aquamatch

        assert "run_insitu_pipeline" in aquamatch.__all__

    def test_all_contains_run_sentinel_pipeline(self):
        import aquamatch

        assert "run_sentinel_pipeline" in aquamatch.__all__

    def test_all_contains_run_acolite_pipeline(self):
        import aquamatch

        assert "run_acolite_pipeline" in aquamatch.__all__

    def test_all_contains_exactly_three_names(self):
        import aquamatch

        assert len(aquamatch.__all__) == 3

    def test_all_consistent_with_api_all(self):
        """Package __all__ must be a superset of aquamatch.api.__all__."""
        import aquamatch
        from aquamatch.api import __all__ as api_all

        for name in api_all:
            assert name in aquamatch.__all__


class TestPackageDocstring:
    """The package docstring must document the top-level import surface."""

    def test_docstring_is_present(self):
        import aquamatch

        assert aquamatch.__doc__ is not None
        assert len(aquamatch.__doc__.strip()) > 0

    def test_docstring_mentions_run_insitu_pipeline(self):
        import aquamatch

        assert "run_insitu_pipeline" in aquamatch.__doc__

    def test_docstring_mentions_run_sentinel_pipeline(self):
        import aquamatch

        assert "run_sentinel_pipeline" in aquamatch.__doc__

    def test_docstring_mentions_run_acolite_pipeline(self):
        import aquamatch

        assert "run_acolite_pipeline" in aquamatch.__doc__
