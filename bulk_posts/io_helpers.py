from __future__ import annotations

from pathlib import Path

import pandas as pd
from dotenv import dotenv_values

from .config import DEFAULT_INPUT_CSV
from .errors import CredentialsError


def load_credentials(path: Path) -> str:
    if not path.exists():
        raise CredentialsError(f"Missing .env file at {path}")
    values = dotenv_values(path)
    token = values.get("X_BEARER_TOKEN") or values.get("BEARER_TOKEN")
    if not isinstance(token, str) or not token.strip():
        raise CredentialsError(f"Missing X_BEARER_TOKEN in {path}")
    return token.strip()


def load_usernames(path: Path, max_profiles: int | None) -> list[str]:
    csv_path = path if path.exists() else DEFAULT_INPUT_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    frame = pd.read_csv(csv_path, usecols=["username"])
    usernames = (
        frame["username"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .drop_duplicates()
        .tolist()
    )
    return usernames[:max_profiles] if max_profiles else usernames

