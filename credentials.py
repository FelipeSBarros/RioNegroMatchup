import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class SentinelCredentials:
    sh_client_id: Optional[str] = None
    sh_client_secret: Optional[str] = None
    dataspace_access_key: Optional[str] = None
    dataspace_secret_key: Optional[str] = None
    sh_base_url: str = "https://sh.dataspace.copernicus.eu"
    sh_token_url: str = (
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
        "protocol/openid-connect/token"
    )

    @classmethod
    def from_env(cls) -> "SentinelCredentials":
        return cls(
            sh_client_id=os.getenv("SH_CLIENT_ID"),
            sh_client_secret=os.getenv("SH_CLIENT_SECRET"),
            dataspace_access_key=os.getenv("DATASPACE_ACCESS_KEY"),
            dataspace_secret_key=os.getenv("DATASPACE_SECRET_KEY"),
        )
