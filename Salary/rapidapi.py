"""Job salary fetch via RapidAPI → CSV + Parquet → R2.

Public-CI safe: aggregate counts only in logs; no keys, webhooks, or payloads.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from salary_r2 import (
    configure_logging,
    data_dir,
    discord_user_prefix,
    download_object_if_exists,
    env_float,
    env_int,
    env_list_required,
    env_required,
    env_str,
    fold_upload_results,
    log_r2_object_layout,
    resolve_output_keys,
    r2_object_key,
    salary_r2_prefix,
    s3_client,
    send_discord_alert,
    upload_file_if_changed,
)

logger = logging.getLogger("salary.fetch")

OUTPUT_COLUMNS = [
    "requested_title",
    "requested_location",
    "role_family",
    "seniority_label",
    "location",
    "job_title",
    "min_salary",
    "max_salary",
    "median_salary",
    "min_base_salary",
    "max_base_salary",
    "median_base_salary",
    "min_additional_pay",
    "max_additional_pay",
    "median_additional_pay",
    "salary_period",
    "salary_currency",
    "salary_count",
    "salaries_updated_at",
    "publisher_name",
    "publisher_link",
    "confidence",
    "fetched_at_utc",
]

SENIORITY_PATTERN = re.compile(r"\b(jr\.?|junior|sr\.?|senior)\b", re.IGNORECASE)
ROMAN_LEVEL_PATTERN = re.compile(r"\b(III|II)\b")
POWER_BI_PATTERN = re.compile(r"power\s*bi", re.IGNORECASE)
FAILOVER_STATUS_CODES = {401, 403, 429}
RATE_LIMIT_EXIT_PER_KEY = 2


class AllKeysRateLimited(RuntimeError):
    """Every API key in the pool returned HTTP 429 at least twice."""


@dataclass
class KeyStats:
    attempts: int = 0
    successes: int = 0
    rate_limited: int = 0
    auth_fail: int = 0


@dataclass
class ApiKeyRotator:
    keys: list[str]
    _cursor: int = 0
    stats: dict[int, KeyStats] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.stats = {i: KeyStats() for i in range(len(self.keys))}

    def pick_start_index(self) -> int:
        idx = self._cursor % len(self.keys)
        self._cursor += 1
        return idx

    def key_indices_for_attempt(self, start: int) -> list[int]:
        return [(start + offset) % len(self.keys) for offset in range(len(self.keys))]

    def all_keys_rate_limited_enough(self, threshold: int = RATE_LIMIT_EXIT_PER_KEY) -> bool:
        return bool(self.keys) and all(
            self.stats[i].rate_limited >= threshold for i in range(len(self.keys))
        )


def load_api_keys() -> list[str]:
    pool_raw = (env_str("RAPIDAPI_KEYS", "") or "").strip()
    single = (env_str("RAPIDAPI_KEY", "") or "").strip()
    keys: list[str] = []
    if pool_raw:
        keys.extend(k.strip() for k in pool_raw.split(",") if k.strip())
    if single and single not in keys:
        keys.append(single)
    if not keys:
        logger.error("At least one RapidAPI key is required (RAPIDAPI_KEYS or RAPIDAPI_KEY).")
        sys.exit(1)
    logger.info("Loaded %d RapidAPI key(s) for rotation.", len(keys))
    return keys


def normalize_title(title: str) -> str:
    cleaned = POWER_BI_PATTERN.sub("Power BI", title.strip())
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def infer_seniority(title: str) -> str:
    level_match = ROMAN_LEVEL_PATTERN.search(title)
    if level_match:
        return "L3" if level_match.group(1).upper() == "III" else "L2"
    match = SENIORITY_PATTERN.search(title)
    if not match:
        return "MID"
    token = match.group(1).lower().rstrip(".")
    if token in {"jr", "junior"}:
        return "JR"
    return "SR"


def strip_title_tokens(title: str) -> str:
    without = ROMAN_LEVEL_PATTERN.sub("", title)
    without = SENIORITY_PATTERN.sub("", without)
    return re.sub(r"\s+", " ", without).strip()


def load_role_family_aliases() -> dict[str, str]:
    """Optional ``Alias=Canonical|...`` map (keys normalized to lowercase)."""
    raw = env_str("SALARY_ROLE_FAMILY_ALIASES", "")
    if not raw:
        return {}
    aliases: dict[str, str] = {}
    for part in raw.split("|"):
        piece = part.strip()
        if "=" not in piece:
            continue
        alias, canonical = piece.split("=", 1)
        alias_key = alias.strip().lower()
        canonical_val = canonical.strip()
        if alias_key and canonical_val:
            aliases[alias_key] = canonical_val
    return aliases


def infer_role_family(
    title: str,
    families: list[str],
    aliases: dict[str, str],
) -> str:
    base = normalize_title(strip_title_tokens(title))
    lower = base.lower()
    if lower in aliases:
        return aliases[lower]
    for family in sorted(families, key=len, reverse=True):
        if family.lower() in lower:
            return family
    return base or "Unknown"


def log_quota_plan(roles: list[str], locations: list[str], key_count: int) -> int:
    planned = len(roles) * len(locations)
    quota_per_key = env_int("RAPIDAPI_MONTHLY_QUOTA_PER_KEY", 50)
    capacity = key_count * quota_per_key
    logger.info(
        "Quota plan — roles=%d locations=%d planned_requests=%d keys=%d estimated_capacity=%d",
        len(roles),
        len(locations),
        planned,
        key_count,
        capacity,
    )
    if planned > capacity:
        logger.warning(
            "Planned requests (%d) exceed estimated monthly capacity (%d).",
            planned,
            capacity,
        )
    return planned


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_salary_with_rotation(
    rotator: ApiKeyRotator,
    *,
    url: str,
    host: str,
    job_title: str,
    location: str,
    location_type: str,
    timeout_sec: float,
) -> list[dict[str, Any]] | None:
    start_idx = rotator.pick_start_index()
    last_status: int | None = None

    for key_idx in rotator.key_indices_for_attempt(start_idx):
        stats = rotator.stats[key_idx]
        stats.attempts += 1
        headers = {
            "x-rapidapi-host": host,
            "x-rapidapi-key": rotator.keys[key_idx],
        }
        params = {
            "job_title": job_title,
            "location": location,
            "location_type": location_type,
        }
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout_sec)
            last_status = response.status_code
            if response.status_code in FAILOVER_STATUS_CODES:
                if response.status_code == 429:
                    stats.rate_limited += 1
                else:
                    stats.auth_fail += 1
                continue
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(rows, list) and rows:
                stats.successes += 1
                return rows
            return None
        except requests.RequestException:
            continue

    if last_status is not None:
        logger.warning("Query failed after key rotation (HTTP %s).", last_status)
    else:
        logger.warning("Query failed after key rotation (transport error).")
    return None


def enrich_rows(
    rows: list[dict[str, Any]],
    *,
    requested_title: str,
    requested_location: str,
    fetched_at: str,
    role_families: list[str],
    role_family_aliases: dict[str, str],
) -> list[dict[str, Any]]:
    role_family = infer_role_family(
        requested_title, role_families, role_family_aliases
    )
    seniority = infer_seniority(requested_title)
    enriched: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        record = {col: row.get(col) for col in OUTPUT_COLUMNS if col not in {
            "requested_title",
            "requested_location",
            "role_family",
            "seniority_label",
            "fetched_at_utc",
        }}
        record["requested_title"] = requested_title
        record["requested_location"] = requested_location
        record["role_family"] = role_family
        record["seniority_label"] = seniority
        record["fetched_at_utc"] = fetched_at
        enriched.append(record)
    return enriched


def load_baseline_dataframe(
    csv_path: Path,
    parquet_path: Path,
    client,
    bucket: str,
    key_csv: str,
    key_parquet: str,
) -> pd.DataFrame:
    """Load baseline from R2 (required for CI) or existing local copies."""
    if download_object_if_exists(client, bucket, key_parquet, parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            logger.info("Loaded baseline from R2 parquet (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read downloaded R2 parquet; trying csv.")

    if download_object_if_exists(client, bucket, key_csv, csv_path):
        try:
            df = pd.read_csv(csv_path)
            logger.info("Loaded baseline from R2 csv (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read downloaded R2 csv.")

    if parquet_path.is_file():
        try:
            df = pd.read_parquet(parquet_path)
            logger.info("Loaded baseline from local parquet (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read local parquet baseline.")

    if csv_path.is_file():
        try:
            df = pd.read_csv(csv_path)
            logger.info("Loaded baseline from local csv (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read local csv baseline.")

    logger.error(
        "Baseline not found in R2 under configured prefix; upload raw parquet/csv then re-run."
    )
    sys.exit(1)


def merge_with_baseline(baseline: pd.DataFrame, new_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not new_rows and len(baseline) == 0:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    if not new_rows:
        return baseline
    baseline_records = baseline.to_dict(orient="records") if len(baseline) > 0 else []
    return build_dataframe(baseline_records + new_rows)


def build_dataframe(all_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not all_rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    df = pd.DataFrame(all_rows)
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[OUTPUT_COLUMNS]
    # Keep one row per monthly snapshot; do not collapse prior months' rows.
    dedupe_cols = [
        "requested_title",
        "requested_location",
        "job_title",
        "location",
        "publisher_name",
        "salaries_updated_at",
        "fetched_at_utc",
    ]
    df = df.drop_duplicates(subset=dedupe_cols, keep="first").reset_index(drop=True)
    return df


def log_key_summary(rotator: ApiKeyRotator) -> None:
    for idx, stats in rotator.stats.items():
        logger.info(
            "Key slot %d — attempts=%d successes=%d rate_limited=%d auth_fail=%d",
            idx + 1,
            stats.attempts,
            stats.successes,
            stats.rate_limited,
            stats.auth_fail,
        )


def collect_salary_data(
    rotator: ApiKeyRotator,
    *,
    url: str,
    host: str,
    roles: list[str],
    locations: list[str],
    role_families: list[str],
    role_family_aliases: dict[str, str],
    location_type: str,
    delay_sec: float,
    timeout_sec: float,
    max_queries: int = 0,
) -> tuple[list[dict[str, Any]], int, int]:
    all_rows: list[dict[str, Any]] = []
    planned_queries = len(roles) * len(locations)
    success_queries = 0
    attempted_queries = 0

    for location in locations:
        for title in roles:
            if max_queries > 0 and attempted_queries >= max_queries:
                logger.warning("Stopped at SALARY_MAX_QUERIES_PER_RUN=%d.", max_queries)
                break
            attempted_queries += 1
            fetched_at = utc_now_iso()
            rows = fetch_salary_with_rotation(
                rotator,
                url=url,
                host=host,
                job_title=title,
                location=location,
                location_type=location_type,
                timeout_sec=timeout_sec,
            )
            if rotator.all_keys_rate_limited_enough():
                log_key_summary(rotator)
                logger.error(
                    "All %d RapidAPI keys hit HTTP 429 at least %d times; stopping.",
                    len(rotator.keys),
                    RATE_LIMIT_EXIT_PER_KEY,
                )
                raise AllKeysRateLimited()
            if rows:
                success_queries += 1
                all_rows.extend(
                    enrich_rows(
                        rows,
                        requested_title=title,
                        requested_location=location,
                        fetched_at=fetched_at,
                        role_families=role_families,
                        role_family_aliases=role_family_aliases,
                    )
                )
            if delay_sec > 0:
                time.sleep(delay_sec)
        else:
            continue
        break

    logger.info(
        "Fetch complete — planned=%d attempted=%d successful=%d rows=%d",
        planned_queries,
        attempted_queries,
        success_queries,
        len(all_rows),
    )
    return all_rows, planned_queries, success_queries


def write_outputs(df: pd.DataFrame, csv_path, parquet_path) -> None:
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)
    logger.info("Wrote local outputs (%d rows).", len(df))


def upload_outputs(client, bucket: str, prefix: str, csv_path, parquet_path, csv_key: str, parquet_key: str) -> str:
    key_csv = r2_object_key(prefix, csv_key)
    key_parquet = r2_object_key(prefix, parquet_key)
    csv_result = upload_file_if_changed(
        client,
        bucket,
        key_csv,
        csv_path,
        content_type="text/csv",
        public=True,
    )
    parquet_result = upload_file_if_changed(
        client,
        bucket,
        key_parquet,
        parquet_path,
        content_type="application/vnd.apache.parquet",
        public=True,
    )
    return fold_upload_results(csv_result, parquet_result)


def main() -> tuple[str, int, int, int]:
    configure_logging()

    url = env_required("RAPIDAPI_URL")
    host = env_required("RAPIDAPI_HOST")
    location_type = env_str("SALARY_LOCATION_TYPE", "CITY")
    delay_sec = env_float("SALARY_REQUEST_DELAY_SEC", 1.0)
    timeout_sec = env_float("SALARY_REQUEST_TIMEOUT_SEC", 10.0)

    roles = env_list_required("SALARY_ROLE_VARIANTS")
    locations = env_list_required("SALARY_LOCATIONS")
    role_families = env_list_required("SALARY_ROLE_FAMILIES")
    role_family_aliases = load_role_family_aliases()

    parquet_basename, csv_basename = resolve_output_keys(
        "R2_SALARY_PARQUET_KEY", "R2_SALARY_CSV_KEY"
    )

    base = data_dir()
    csv_path = base / csv_basename
    parquet_path = base / parquet_basename
    client, bucket = s3_client()
    prefix = salary_r2_prefix()
    key_csv = r2_object_key(prefix, csv_basename)
    key_parquet = r2_object_key(prefix, parquet_basename)
    log_r2_object_layout(prefix)

    baseline = load_baseline_dataframe(
        csv_path, parquet_path, client, bucket, key_csv, key_parquet
    )

    rotator = ApiKeyRotator(keys=load_api_keys())
    max_queries = env_int("SALARY_MAX_QUERIES_PER_RUN", 0)
    log_quota_plan(roles, locations, len(rotator.keys))
    all_rows, planned_queries, success_queries = collect_salary_data(
        rotator,
        url=url,
        host=host,
        roles=roles,
        locations=locations,
        role_families=role_families,
        role_family_aliases=role_family_aliases,
        location_type=location_type,
        delay_sec=delay_sec,
        timeout_sec=timeout_sec,
        max_queries=max_queries,
    )
    log_key_summary(rotator)

    if success_queries == 0:
        logger.error("All salary queries failed.")
        return "failed", len(baseline), planned_queries, 0

    df = merge_with_baseline(baseline, all_rows)
    write_outputs(df, csv_path, parquet_path)

    upload_status = upload_outputs(
        client,
        bucket,
        prefix,
        csv_path,
        parquet_path,
        csv_basename,
        parquet_basename,
    )
    if upload_status == "error":
        logger.error("R2 upload failed.")
        return "failed", len(df), planned_queries, success_queries
    return upload_status, len(df), planned_queries, success_queries


if __name__ == "__main__":
    ts = int(time.time())
    label = "Salary Fetch"
    pre = discord_user_prefix()
    try:
        result, row_count, planned_queries, success_queries = main()
        query_summary = f"planned={planned_queries} ok={success_queries}"
        if result == "uploaded":
            send_discord_alert(
                f"✅ {label} — Uploaded at <t:{ts}:f> | rows={row_count} {query_summary}"
            )
        elif result == "no-change":
            send_discord_alert(
                f"✅ {label} — No changes at <t:{ts}:f> | rows={row_count} {query_summary}"
            )
        elif result == "failed":
            send_discord_alert(f"{pre}❌ {label} failed at <t:{ts}:f>")
            sys.exit(1)
        else:
            send_discord_alert(
                f"{pre}⚠️ {label} — Finished with status {result} at <t:{ts}:f>"
            )
            sys.exit(1)
    except AllKeysRateLimited:
        send_discord_alert(
            f"{pre}❌ {label} — all keys rate limited (HTTP 429×{RATE_LIMIT_EXIT_PER_KEY}) at <t:{ts}:f>"
        )
        sys.exit(1)
    except Exception:
        logger.exception("Script failed.")
        send_discord_alert(f"{pre}❌ {label} failed at <t:{ts}:f>")
        sys.exit(1)
