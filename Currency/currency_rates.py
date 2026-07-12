#!/usr/bin/env python3
"""Monthly USD-base FX rates → CSV + Parquet → R2."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

BASE_CURRENCY = "USD"
API_URL_TEMPLATE = "https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"

CSV_COLUMNS = [
    "year_month",
    "base_currency",
    "target_currency",
    "units_per_usd",
    "fetched_at_utc",
    "api_time_last_update_utc",
]

ISO_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Env values redacted from logs and Discord before any output leaves the process.
_REDACT_ENV_NAMES = (
    "EXCHANGERATE_API_KEY",
    "CURRENCY_R2_PREFIX",
    "R2_BUCKET",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_ENDPOINT",
    "R2_CURRENCY_RATES_CSV_KEY",
    "R2_CURRENCY_RATES_PARQUET_KEY",
    "DISCORD_WEBHOOK_URL",
    "DISCORD_USER_ID",
    "EXTRA_CURRENCIES",
)


def redact_secrets(text: str) -> str:
    """Strip known env values (and EXTRA_CURRENCIES codes) from outbound text."""
    result = text
    for name in _REDACT_ENV_NAMES:
        value = (os.getenv(name) or "").strip()
        if not value:
            continue
        if name == "EXTRA_CURRENCIES":
            for part in re.split(r"[;,]", value):
                code = part.strip()
                if len(code) >= 3:
                    result = result.replace(code, "***")
            continue
        if len(value) >= 4:
            result = result.replace(value, "***")
    return result


def safe_log(message: str) -> None:
    print(redact_secrets(message))


def load_local_env() -> None:
    """Load gitignored dev.env or .env for local runs; CI uses injected env."""
    if os.path.isfile("dev.env"):
        load_dotenv("dev.env")
    else:
        load_dotenv()


def require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(
            f"Missing required environment variable {name!r}. "
            "Set it in dev.env (local) or GitHub prod environment secrets (CI). "
            "See .env.example."
        )
    return value


def parse_extra_currencies(raw: str) -> set[str]:
    """Parse EXTRA_CURRENCIES secret; uppercase ISO codes, no USD."""
    parts = re.split(r"[;,]", raw)
    codes: set[str] = set()
    for part in parts:
        code = part.strip().upper()
        if not code:
            continue
        if not ISO_CURRENCY_RE.match(code):
            raise ValueError("Invalid currency code in EXTRA_CURRENCIES")
        if code == BASE_CURRENCY:
            raise ValueError("EXTRA_CURRENCIES must not include USD (base is always USD)")
        codes.add(code)
    if not codes:
        raise ValueError("EXTRA_CURRENCIES is empty after parsing")
    return codes


def get_target_currencies() -> set[str]:
    extras = parse_extra_currencies(require_env("EXTRA_CURRENCIES"))
    return {BASE_CURRENCY} | extras


def r2_object_key(prefix: str, filename: str) -> str:
    if not prefix:
        return filename
    return f"{prefix}/{filename}"


def currency_r2_prefix() -> str:
    """Folder prefix inside the bucket (never bucket root for Currency scripts)."""
    raw = (os.getenv("CURRENCY_R2_PREFIX") or "").strip().strip("/")
    if not raw:
        raise ValueError(
            "Missing CURRENCY_R2_PREFIX — Currency outputs must not upload to bucket root. "
            "Set it in dev.env (local) or GitHub prod environment secrets (CI). "
            "See .env.example."
        )
    return raw


def resolve_output_basenames() -> tuple[str, str]:
    """Parquet basename required; csv basename optional (derived from parquet stem)."""
    parquet_basename = require_env("R2_CURRENCY_RATES_PARQUET_KEY")
    csv_raw = (os.getenv("R2_CURRENCY_RATES_CSV_KEY") or "").strip()
    csv_basename = csv_raw if csv_raw else f"{Path(parquet_basename).stem}.csv"
    return csv_basename, parquet_basename


def data_dir() -> Path:
    override = (os.getenv("CURRENCY_DATA_DIR") or "").strip()
    base = Path(override).expanduser() if override else Path(__file__).resolve().parent
    base.mkdir(parents=True, exist_ok=True)
    return base


def get_file_hash(path: str) -> str | None:
    if not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(4096), b""):
            digest.update(chunk)
    return digest.hexdigest()


def send_discord_alert(message: str) -> None:
    webhook = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()
    if not webhook:
        return
    user_id = (os.getenv("DISCORD_USER_ID") or "").strip()
    content = message
    if user_id:
        content = f"<@{user_id}> {message}"
    try:
        response = requests.post(
            webhook,
            data=json.dumps({"content": content}),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as exc:
        safe_log(f"Failed to send Discord alert: {exc}")


def create_s3_client():
    bucket = require_env("R2_BUCKET")
    access_key = require_env("R2_ACCESS_KEY_ID")
    secret_key = require_env("R2_SECRET_ACCESS_KEY")
    endpoint = require_env("R2_ENDPOINT")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
    ), bucket


def download_from_r2(client, bucket: str, object_key: str, local_path: str) -> str:
    """
    Returns:
        ok — downloaded
        missing — object not found (bootstrap)
        error — other failure
    """
    try:
        client.download_file(bucket, object_key, local_path)
        print(f"Downloaded object from R2")
        return "ok"
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code", "") or ""
        if code in ("404", "NoSuchKey", "NotFound"):
            print("R2 object not found (bootstrap)")
            return "missing"
        safe_log(f"R2 download failed: {exc}")
        return "error"
    except Exception as exc:
        safe_log(f"R2 download failed: {exc}")
        return "error"


def upload_to_r2(client, bucket: str, local_path: str, object_key: str) -> bool:
    try:
        with open(local_path, "rb") as handle:
            client.upload_fileobj(handle, bucket, object_key)
        print("Uploaded to R2")
        return True
    except Exception as exc:
        safe_log(f"R2 upload failed: {exc}")
        return False


def read_rates_csv(path: str) -> pd.DataFrame:
    if os.path.isfile(path):
        return pd.read_csv(path, encoding="utf-8-sig")
    return pd.DataFrame(columns=CSV_COLUMNS)


def month_already_captured(df: pd.DataFrame, year_month: str) -> bool:
    if df.empty or "year_month" not in df.columns:
        return False
    return (df["year_month"].astype(str) == year_month).any()


def fetch_rates(api_key: str, targets: set[str]) -> tuple[dict[str, float], str]:
    url = API_URL_TEMPLATE.format(api_key=api_key)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        raise RuntimeError("ExchangeRate-API request failed") from None
    payload: dict[str, Any] = response.json()

    if payload.get("result") != "success":
        raise RuntimeError(f"ExchangeRate-API error: {payload.get('error-type', 'unknown')}")

    if payload.get("base_code") != BASE_CURRENCY:
        raise RuntimeError(
            f"Unexpected base_code {payload.get('base_code')!r}; expected {BASE_CURRENCY}"
        )

    conversion_rates: dict[str, Any] = payload.get("conversion_rates") or {}
    rates: dict[str, float] = {}
    missing = []
    for code in sorted(targets):
        if code == BASE_CURRENCY:
            rates[code] = 1.0
            continue
        if code not in conversion_rates:
            missing.append(code)
            continue
        rates[code] = float(conversion_rates[code])

    if missing:
        raise RuntimeError(
            f"API response missing {len(missing)} target currency rate(s)"
        )

    api_updated = str(payload.get("time_last_update_utc") or "")
    return rates, api_updated


def build_rows(
    year_month: str,
    targets: set[str],
    rates: dict[str, float],
    fetched_at: datetime,
    api_updated_at: str,
) -> list[dict[str, Any]]:
    fetched_iso = fetched_at.isoformat()
    rows = []
    for code in sorted(targets):
        rows.append(
            {
                "year_month": year_month,
                "base_currency": BASE_CURRENCY,
                "target_currency": code,
                "units_per_usd": rates[code],
                "fetched_at_utc": fetched_iso,
                "api_time_last_update_utc": api_updated_at,
            }
        )
    return rows


def write_csv_and_parquet(df: pd.DataFrame, csv_path: str, parquet_path: str) -> None:
    df.to_csv(csv_path, index=False, encoding="utf-8")
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, parquet_path)


def is_ci() -> bool:
    return (
        os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
        or os.environ.get("CI", "").lower() == "true"
    )


def main() -> str:
    load_local_env()

    prefix = currency_r2_prefix()
    csv_basename, parquet_basename = resolve_output_basenames()
    r2_csv_key = r2_object_key(prefix, csv_basename)
    r2_parquet_key = r2_object_key(prefix, parquet_basename)
    work = data_dir()
    csv_path = str(work / csv_basename)
    parquet_path = str(work / parquet_basename)
    api_key = require_env("EXCHANGERATE_API_KEY")

    targets = get_target_currencies()
    year_month = datetime.now(timezone.utc).strftime("%Y-%m")

    client, bucket = create_s3_client()

    print("\n=== Pulling from R2 ===\n")
    pull_status = download_from_r2(client, bucket, r2_csv_key, csv_path)
    if pull_status == "error" and is_ci():
        raise RuntimeError("Strict R2 pull failed in CI; refusing to continue")
    if pull_status == "ok":
        hash_before = get_file_hash(csv_path)
    else:
        hash_before = None

    df = read_rates_csv(csv_path)

    if month_already_captured(df, year_month):
        print(f"Already captured {year_month}, skipping")
        return "skipped"

    print(f"\n=== Fetching rates for {year_month} (base {BASE_CURRENCY}) ===\n")
    rates, api_updated_at = fetch_rates(api_key, targets)
    fetched_at = datetime.now(timezone.utc)
    new_rows = build_rows(year_month, targets, rates, fetched_at, api_updated_at)

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    write_csv_and_parquet(df, csv_path, parquet_path)

    hash_after = get_file_hash(csv_path)
    if hash_before == hash_after:
        print("No CSV hash change; skipping upload")
        return "no-change"

    print("\n=== Uploading to R2 ===\n")
    csv_ok = upload_to_r2(client, bucket, csv_path, r2_csv_key)
    parquet_ok = upload_to_r2(client, bucket, parquet_path, r2_parquet_key)
    if not (csv_ok and parquet_ok):
        raise RuntimeError("R2 upload failed")

    print(f"Captured {len(new_rows)} rate row(s) for {year_month}")
    return "uploaded"


if __name__ == "__main__":
    timestamp = int(time.time())
    exit_code = 0
    try:
        main()
    except Exception as exc:
        safe_log(f"Currency rates pipeline failed: {exc}")
        send_discord_alert(
            redact_secrets(
                f"Currency rates pipeline failed at <t:{timestamp}:f>: {exc}"
            )
        )
        exit_code = 1
    sys.exit(exit_code)
