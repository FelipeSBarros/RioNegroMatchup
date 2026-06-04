from unittest.mock import MagicMock, patch

# Prevent the module-level Client.open() network call in sentinel_data.py
# from firing during test collection.
_pystac_patch = patch(
    "pystac_client.Client.open",
    return_value=MagicMock(),
)
_pystac_patch.start()
