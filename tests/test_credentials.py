"""
Unit tests for aquamatch/credentials.py — SentinelCredentials.

Conventions (matching the existing test suite):
- One class per logical unit under test.
- monkeypatch.setenv / delenv for environment variable control — no real
  network or filesystem access needed.
- pytest.approx is not needed here (no floats); plain assert throughout.
"""

from __future__ import annotations

import pytest

from aquamatch.credentials import SentinelCredentials

# ---------------------------------------------------------------------------
# Defaults / construction
# ---------------------------------------------------------------------------


class TestSentinelCredentialsDefaults:
    """Tests for default field values when constructed with no arguments."""

    def test_all_secret_fields_default_to_none(self):
        creds = SentinelCredentials()
        assert creds.sh_client_id is None
        assert creds.sh_client_secret is None
        assert creds.dataspace_access_key is None
        assert creds.dataspace_secret_key is None

    def test_sh_base_url_has_sensible_default(self):
        creds = SentinelCredentials()
        assert creds.sh_base_url == "https://sh.dataspace.copernicus.eu"

    def test_sh_token_url_has_sensible_default(self):
        creds = SentinelCredentials()
        assert creds.sh_token_url == (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        )

    def test_explicit_values_override_defaults(self):
        creds = SentinelCredentials(
            sh_client_id="id123",
            sh_client_secret="secret123",
            dataspace_access_key="access123",
            dataspace_secret_key="secretkey123",
        )
        assert creds.sh_client_id == "id123"
        assert creds.sh_client_secret == "secret123"
        assert creds.dataspace_access_key == "access123"
        assert creds.dataspace_secret_key == "secretkey123"

    def test_custom_urls_can_be_overridden(self):
        creds = SentinelCredentials(
            sh_base_url="https://custom.example.com",
            sh_token_url="https://custom.example.com/token",
        )
        assert creds.sh_base_url == "https://custom.example.com"
        assert creds.sh_token_url == "https://custom.example.com/token"

    def test_is_dataclass_instance_supports_equality(self):
        """Two instances with identical fields must compare equal (dataclass __eq__)."""
        a = SentinelCredentials(sh_client_id="x")
        b = SentinelCredentials(sh_client_id="x")
        assert a == b

    def test_different_field_values_are_not_equal(self):
        a = SentinelCredentials(sh_client_id="x")
        b = SentinelCredentials(sh_client_id="y")
        assert a != b


# ---------------------------------------------------------------------------
# from_env()
# ---------------------------------------------------------------------------


class TestSentinelCredentialsFromEnv:
    """Tests for SentinelCredentials.from_env()."""

    _ENV_VARS = (
        "SH_CLIENT_ID",
        "SH_CLIENT_SECRET",
        "DATASPACE_ACCESS_KEY",
        "DATASPACE_SECRET_KEY",
    )

    def _clear_env(self, monkeypatch):
        for var in self._ENV_VARS:
            monkeypatch.delenv(var, raising=False)

    def test_reads_all_four_env_vars(self, monkeypatch):
        self._clear_env(monkeypatch)
        monkeypatch.setenv("SH_CLIENT_ID", "env-id")
        monkeypatch.setenv("SH_CLIENT_SECRET", "env-secret")
        monkeypatch.setenv("DATASPACE_ACCESS_KEY", "env-access")
        monkeypatch.setenv("DATASPACE_SECRET_KEY", "env-secretkey")

        creds = SentinelCredentials.from_env()

        assert creds.sh_client_id == "env-id"
        assert creds.sh_client_secret == "env-secret"
        assert creds.dataspace_access_key == "env-access"
        assert creds.dataspace_secret_key == "env-secretkey"

    def test_missing_env_vars_default_to_none(self, monkeypatch):
        self._clear_env(monkeypatch)
        creds = SentinelCredentials.from_env()
        assert creds.sh_client_id is None
        assert creds.sh_client_secret is None
        assert creds.dataspace_access_key is None
        assert creds.dataspace_secret_key is None

    def test_partial_env_vars_set(self, monkeypatch):
        """Only SH_* set — DATASPACE_* must remain None, not raise."""
        self._clear_env(monkeypatch)
        monkeypatch.setenv("SH_CLIENT_ID", "only-id")
        monkeypatch.setenv("SH_CLIENT_SECRET", "only-secret")

        creds = SentinelCredentials.from_env()

        assert creds.sh_client_id == "only-id"
        assert creds.sh_client_secret == "only-secret"
        assert creds.dataspace_access_key is None
        assert creds.dataspace_secret_key is None

    def test_from_env_does_not_override_urls(self, monkeypatch):
        """from_env() only touches the four secret fields; URLs keep their
        dataclass defaults (there are no SH_BASE_URL / SH_TOKEN_URL env vars
        in the original module-level code)."""
        self._clear_env(monkeypatch)
        creds = SentinelCredentials.from_env()
        assert creds.sh_base_url == "https://sh.dataspace.copernicus.eu"
        assert creds.sh_token_url == (
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
            "protocol/openid-connect/token"
        )

    def test_from_env_returns_sentinel_credentials_instance(self, monkeypatch):
        self._clear_env(monkeypatch)
        creds = SentinelCredentials.from_env()
        assert isinstance(creds, SentinelCredentials)

    def test_empty_string_env_var_is_preserved_not_coerced_to_none(self, monkeypatch):
        """An explicitly empty env var ('') is a valid (if unusual) value and
        must be passed through as-is, not silently converted to None —
        os.getenv only returns None when the var is truly unset."""
        self._clear_env(monkeypatch)
        monkeypatch.setenv("SH_CLIENT_ID", "")
        creds = SentinelCredentials.from_env()
        assert creds.sh_client_id == ""

    def test_from_env_independent_calls_do_not_share_state(self, monkeypatch):
        """Two successive from_env() calls under different env states must
        not leak values from one call to the next (guards against accidental
        caching / mutable-default bugs)."""
        self._clear_env(monkeypatch)
        monkeypatch.setenv("SH_CLIENT_ID", "first")
        first = SentinelCredentials.from_env()

        monkeypatch.setenv("SH_CLIENT_ID", "second")
        second = SentinelCredentials.from_env()

        assert first.sh_client_id == "first"
        assert second.sh_client_id == "second"


# ---------------------------------------------------------------------------
# Explicit construction takes precedence over env (integration-style check)
# ---------------------------------------------------------------------------


class TestSentinelCredentialsPrecedence:
    """
    Confirms the intended usage pattern: explicit construction bypasses the
    environment entirely, which is the whole point of Task 1 (Colab support).
    """

    def test_explicit_construction_ignores_env_vars(self, monkeypatch):
        monkeypatch.setenv("SH_CLIENT_ID", "should-not-be-used")
        creds = SentinelCredentials(sh_client_id="explicit-id")
        assert creds.sh_client_id == "explicit-id"

    def test_from_env_and_explicit_produce_independent_instances(self, monkeypatch):
        monkeypatch.setenv("SH_CLIENT_ID", "env-id")
        env_creds = SentinelCredentials.from_env()
        explicit_creds = SentinelCredentials(sh_client_id="explicit-id")

        assert env_creds.sh_client_id == "env-id"
        assert explicit_creds.sh_client_id == "explicit-id"
        assert env_creds != explicit_creds
