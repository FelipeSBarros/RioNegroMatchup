from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch


@dataclass
class SentinelCredentials:
    sh_client_id: Optional[str] = None
    sh_client_secret: Optional[str] = None
    dataspace_access_key: Optional[str] = None
    dataspace_secret_key: Optional[str] = None
    sh_base_url: str = "https://sh.dataspace.copernicus.eu"
    sh_token_url: str = "https://token.example.com"


def build_catalog(
    csv_file, output_json, time_delta=1, cloud_cover=10, credentials=None
):
    raise RuntimeError("must be patched")


def run_download(
    catalog_json,
    output_dir,
    strategy="best",
    max_per_date=1,
    max_cloud_cover=None,
    download_scl=True,
    s3=None,
    credentials=None,
):
    raise RuntimeError("must be patched")
    return {}


def run_sentinel_pipeline(
    csv=None,
    catalog_json=None,
    output_dir=None,
    time_delta=None,
    cloud_cover=None,
    strategy=None,
    max_per_date=None,
    max_cloud_cover=None,
    download_scl=None,
    mode="all",
    credentials=None,
):
    valid_steps = {"all", "catalog", "download"}
    if mode not in valid_steps:
        raise ValueError(f"Invalid mode value '{mode}'.")

    _credentials = None
    if credentials is not None:
        _credentials = (
            credentials
            if isinstance(credentials, SentinelCredentials)
            else SentinelCredentials(**credentials)
        )

    outputs = {}
    if mode in ("catalog", "all"):
        build_catalog(
            csv_file="x",
            output_json="y",
            time_delta=1,
            cloud_cover=10,
            credentials=_credentials,
        )
        outputs["catalog_json"] = "y"

    if mode in ("download", "all"):
        download_stats = run_download(
            catalog_json="y",
            output_dir="z",
            strategy="best",
            max_per_date=1,
            max_cloud_cover=None,
            download_scl=True,
            credentials=_credentials,
        )
        outputs["download_stats"] = download_stats

    return {"status": "success", "outputs": outputs, "_credentials": _credentials}


# --- repro tests ---


def test_credentials_instance_passed_through_unchanged():
    creds = SentinelCredentials(sh_client_id="explicit-id")
    with patch(__name__ + ".build_catalog") as mock_bc, patch(
        __name__ + ".run_download", return_value={}
    ) as mock_rd:
        result = run_sentinel_pipeline(mode="all", credentials=creds)

    assert result["_credentials"] is creds
    _, bc_kwargs = mock_bc.call_args
    assert bc_kwargs["credentials"] is creds
    _, rd_kwargs = mock_rd.call_args
    assert rd_kwargs["credentials"] is creds


def test_credentials_dict_converted_once_and_reused():
    creds_dict = {"sh_client_id": "dict-id", "dataspace_access_key": "dict-access"}
    with patch(__name__ + ".build_catalog") as mock_bc, patch(
        __name__ + ".run_download", return_value={}
    ) as mock_rd:
        result = run_sentinel_pipeline(mode="all", credentials=creds_dict)

    converted = result["_credentials"]
    assert isinstance(converted, SentinelCredentials)
    assert converted.sh_client_id == "dict-id"
    assert converted.dataspace_access_key == "dict-access"

    _, bc_kwargs = mock_bc.call_args
    _, rd_kwargs = mock_rd.call_args
    # Same object reused for both calls, not reconstructed twice
    assert bc_kwargs["credentials"] is converted
    assert rd_kwargs["credentials"] is converted
    assert bc_kwargs["credentials"] is rd_kwargs["credentials"]


def test_credentials_none_stays_none():
    with patch(__name__ + ".build_catalog") as mock_bc, patch(
        __name__ + ".run_download", return_value={}
    ) as mock_rd:
        run_sentinel_pipeline(mode="all", credentials=None)

    _, bc_kwargs = mock_bc.call_args
    _, rd_kwargs = mock_rd.call_args
    assert bc_kwargs["credentials"] is None
    assert rd_kwargs["credentials"] is None


def test_mode_catalog_only_calls_build_catalog_with_credentials():
    creds = SentinelCredentials(sh_client_id="x")
    with patch(__name__ + ".build_catalog") as mock_bc, patch(
        __name__ + ".run_download"
    ) as mock_rd:
        run_sentinel_pipeline(mode="catalog", credentials=creds)

    mock_bc.assert_called_once()
    mock_rd.assert_not_called()


def test_mode_download_only_calls_run_download_with_credentials():
    creds = SentinelCredentials(sh_client_id="x")
    with patch(__name__ + ".build_catalog") as mock_bc, patch(
        __name__ + ".run_download", return_value={}
    ) as mock_rd:
        run_sentinel_pipeline(mode="download", credentials=creds)

    mock_bc.assert_not_called()
    mock_rd.assert_called_once()
    _, rd_kwargs = mock_rd.call_args
    assert rd_kwargs["credentials"] is creds
