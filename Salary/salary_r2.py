"""Shared R2 helpers for Salary automation scripts.

Public-CI safety: no secrets, endpoints, or tokens in logs.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger("salary")


def load_repo_env() -> None:
    """Load ``.env`` from the Salary folder."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.is_file():
        load_dotenv(env_path, override=False)


def env_required(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        logger.error("Missing or empty env var: %s", name)
        sys.exit(1)
    return v


def env_str(name: str, default: str) -> str:
    v = (os.getenv(name) or "").strip()
    return v if v else default


def env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.error("Env var %s must be an integer.", name)
        sys.exit(1)


def env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.error("Env var %s must be a number.", name)
        sys.exit(1)


def parse_env_list(name: str, default: list[str], *, delimiter: str | None = None) -> list[str]:
    """Parse a list from env. Uses ``|`` when present, else comma."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    return _split_env_list(raw, delimiter)


def env_list_required(name: str, *, delimiter: str | None = None) -> list[str]:
    """Require a non-empty list in env (no code defaults — safe for public repos)."""
    raw = env_required(name)
    items = _split_env_list(raw, delimiter)
    if not items:
        logger.error("Env var %s must contain at least one value.", name)
        sys.exit(1)
    return items


def _split_env_list(raw: str, delimiter: str | None = None) -> list[str]:
    sep = delimiter or ("|" if "|" in raw else ",")
    return [part.strip() for part in raw.split(sep) if part.strip()]


def r2_object_key(prefix: str, filename: str) -> str:
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


def csv_basename_from_parquet_key(parquet_key: str) -> str:
    return f"{Path(parquet_key).stem}.csv"


def fold_upload_results(*results: str) -> str:
    if any(r == "error" for r in results):
        return "error"
    if any(r == "uploaded" for r in results):
        return "uploaded"
    return "no-change"


def salary_r2_prefix() -> str:
    """Folder prefix inside the bucket (never bucket root for Salary scripts)."""
    raw = (os.getenv("SALARY_R2_PREFIX") or os.getenv("R2_PREFIX") or "").strip().strip("/")
    if not raw:
        logger.error(
            "Set SALARY_R2_PREFIX (e.g. Salary) or a non-empty R2_PREFIX — "
            "Salary outputs must not upload to bucket root."
        )
        sys.exit(1)
    return raw


def r2_prefix() -> str:
    """Alias for :func:`salary_r2_prefix` (backward compatible name)."""
    return salary_r2_prefix()


def log_r2_object_layout(prefix: str, csv_basename: str, parquet_basename: str) -> None:
    """Public-safe: prefix and basenames only (no bucket/credentials)."""
    logger.info(
        "R2 layout — prefix=%s csv=%s parquet=%s",
        prefix,
        r2_object_key(prefix, csv_basename),
        r2_object_key(prefix, parquet_basename),
    )


def data_dir() -> Path:
    override = os.getenv("SALARY_DATA_DIR")
    base = Path(override).expanduser() if override else Path(__file__).resolve().parent
    base.mkdir(parents=True, exist_ok=True)
    return base


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        logger.error("Missing required environment variable.")
        sys.exit(1)
    return val


def s3_client() -> Tuple["boto3.client", str]:
    bucket = _require("R2_BUCKET")
    access = _require("R2_ACCESS_KEY_ID")
    secret = _require("R2_SECRET_ACCESS_KEY")
    endpoint = _require("R2_ENDPOINT")
    session = boto3.session.Session()
    client = session.client(
        "s3",
        region_name="auto",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
    )
    return client, bucket


def download_object_if_exists(client, bucket: str, key: str, dest: Path) -> bool:
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(dest))
        logger.info("Downloaded baseline from R2 key=%s", key)
        return True
    except ClientError as e:
        code = ""
        try:
            code = e.response.get("Error", {}).get("Code", "") or ""
        except AttributeError:
            code = ""
        if code in {"404", "NoSuchKey", "NotFound"}:
            logger.info("R2 object not found (key=%s).", key)
            return False
        logger.warning("Baseline object fetch failed (%s).", type(e).__name__)
        return False
    except (BotoCoreError, OSError):
        logger.warning("Baseline object fetch transport error.")
        return False


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = ""
        try:
            code = e.response.get("Error", {}).get("Code", "") or ""
        except AttributeError:
            code = ""
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        return False
    except (BotoCoreError, OSError):
        return False


def compute_file_hash(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_file_if_changed(
    client,
    bucket: str,
    key: str,
    local: Path,
    content_type: str = "application/octet-stream",
    cache_control: Optional[str] = None,
    public: bool = True,
) -> str:
    local_hash = compute_file_hash(local)
    if not local_hash:
        return "no-data"

    remote_hash: Optional[str] = None
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        remote_hash = hashlib.sha256(obj["Body"].read()).hexdigest()
    except Exception:
        remote_hash = None

    if local_hash == remote_hash:
        logger.info("No change for object; skipping upload.")
        return "no-change"

    extra = {"ContentType": content_type}
    if public:
        extra["ACL"] = "public-read"
    if cache_control:
        extra["CacheControl"] = cache_control

    try:
        client.upload_file(str(local), bucket, key, ExtraArgs=extra)
        logger.info("Uploaded object to R2.")
        return "uploaded"
    except (ClientError, BotoCoreError, OSError) as e:
        logger.error("R2 upload failed (%s).", type(e).__name__)
        return "error"


def send_discord_alert(message: str) -> None:
    url = os.getenv("DISCORD_WEBHOOK_URL") or os.getenv("DISCORD_WEBHOOK")
    if not url:
        return
    try:
        import requests

        resp = requests.post(url, json={"content": message[:1900]}, timeout=8)
        resp.raise_for_status()
    except Exception:
        logger.warning("Discord notification failed.")


def discord_user_prefix() -> str:
    uid = (os.getenv("DISCORD_USER_ID") or "").strip()
    return f"<@{uid}> " if uid.isdigit() else ""


load_repo_env()
