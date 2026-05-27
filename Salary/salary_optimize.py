"""Optimize raw R2 salary dataset → normalized final outputs + FX sidecar."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import pandas as pd

from salary_r2 import (
    configure_logging,
    data_dir,
    discord_user_prefix,
    download_object_if_exists,
    env_float,
    env_list_required,
    env_required,
    env_str,
    fold_upload_results,
    log_r2_object_layout,
    r2_object_key,
    resolve_output_keys,
    salary_r2_prefix,
    s3_client,
    send_discord_alert,
    upload_file_if_changed,
)
from salary_transform import (
    apply_usd_columns,
    build_exchange_csv,
    distinct_fetch_dates,
    fetch_fx_rates,
    normalize_periods,
    period_factors_from_env,
)

logger = logging.getLogger("salary.optimize")


def load_raw_dataframe(
    csv_path: Path,
    parquet_path: Path,
    client,
    bucket: str,
    key_csv: str,
    key_parquet: str,
) -> pd.DataFrame:
    if download_object_if_exists(client, bucket, key_parquet, parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            logger.info("Loaded raw from R2 parquet (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read downloaded R2 parquet; trying csv.")

    if download_object_if_exists(client, bucket, key_csv, csv_path):
        try:
            df = pd.read_csv(csv_path)
            logger.info("Loaded raw from R2 csv (%d rows).", len(df))
            return df
        except Exception:
            logger.warning("Could not read downloaded R2 csv.")

    if parquet_path.is_file():
        df = pd.read_parquet(parquet_path)
        logger.info("Loaded raw from local parquet (%d rows).", len(df))
        return df

    if csv_path.is_file():
        df = pd.read_csv(csv_path)
        logger.info("Loaded raw from local csv (%d rows).", len(df))
        return df

    logger.error(
        "Raw salary data not found in R2. Upload configured parquet/csv under SALARY_R2_PREFIX first."
    )
    sys.exit(1)


def upload_outputs(
    client,
    bucket: str,
    prefix: str,
    final_csv: Path,
    final_parquet: Path,
    exchange_csv: Path,
    final_csv_key: str,
    final_parquet_key: str,
    exchange_key: str,
) -> str:
    results = [
        upload_file_if_changed(
            client,
            bucket,
            r2_object_key(prefix, final_csv_key),
            final_csv,
            content_type="text/csv",
            public=True,
        ),
        upload_file_if_changed(
            client,
            bucket,
            r2_object_key(prefix, final_parquet_key),
            final_parquet,
            content_type="application/vnd.apache.parquet",
            public=True,
        ),
        upload_file_if_changed(
            client,
            bucket,
            r2_object_key(prefix, exchange_key),
            exchange_csv,
            content_type="text/csv",
            public=True,
        ),
    ]
    return fold_upload_results(*results)


def main() -> tuple[str, int]:
    configure_logging()

    raw_parquet_key, raw_csv_key = resolve_output_keys(
        "R2_SALARY_PARQUET_KEY", "R2_SALARY_CSV_KEY"
    )
    final_parquet_key, final_csv_key = resolve_output_keys(
        "R2_SALARY_FINAL_PARQUET_KEY", "R2_SALARY_FINAL_CSV_KEY"
    )
    exchange_key = env_required("R2_CURRENCY_EXCHANGE_CSV_KEY")
    fx_api_base = env_str("SALARY_FX_API_BASE", "https://api.frankfurter.app")
    fx_targets = tuple(env_list_required("SALARY_FX_CURRENCIES"))
    logger.info("FX configured for %d non-USD currencies.", len(fx_targets))

    factors = period_factors_from_env(
        hours_per_month=env_float("SALARY_HOURS_PER_MONTH", 2080.0 / 12.0),
        days_per_month=env_float("SALARY_DAYS_PER_MONTH", 5.0 * 52.0 / 12.0),
        weeks_per_month=env_float("SALARY_WEEKS_PER_MONTH", 52.0 / 12.0),
    )

    base = data_dir()
    raw_csv = base / raw_csv_key
    raw_parquet = base / raw_parquet_key
    final_csv = base / final_csv_key
    final_parquet = base / final_parquet_key
    exchange_csv = base / exchange_key

    client, bucket = s3_client()
    prefix = salary_r2_prefix()
    key_raw_csv = r2_object_key(prefix, raw_csv_key)
    key_raw_parquet = r2_object_key(prefix, raw_parquet_key)

    log_r2_object_layout(prefix)

    raw_df = load_raw_dataframe(
        raw_csv, raw_parquet, client, bucket, key_raw_csv, key_raw_parquet
    )
    if len(raw_df) == 0:
        logger.warning("Raw dataset is empty; writing empty final outputs.")

    normalized_df, unknown_counts = normalize_periods(raw_df, factors)
    if unknown_counts:
        logger.info("Rows with unknown period: %d", sum(unknown_counts.values()))

    fetch_dates = distinct_fetch_dates(normalized_df)
    rates_by_date = fetch_fx_rates(
        fetch_dates, fx_targets, api_base=fx_api_base
    )
    if not rates_by_date:
        logger.error("No FX rates available; cannot build USD columns or exchange file.")
        return "failed", len(normalized_df)

    for d in fetch_dates:
        if d in rates_by_date:
            missing_count = sum(1 for c in fx_targets if c not in rates_by_date[d])
            if missing_count:
                logger.warning(
                    "FX incomplete for %s (%d currencies missing).",
                    d.isoformat(),
                    missing_count,
                )

    exchange_df = build_exchange_csv(rates_by_date, fx_targets)
    final_df = apply_usd_columns(normalized_df, rates_by_date)

    final_df.to_parquet(final_parquet, index=False)
    final_df.to_csv(final_csv, index=False)
    exchange_df.to_csv(exchange_csv, index=False)
    logger.info(
        "Wrote final (%d rows) and exchange (%d rows) locally.",
        len(final_df),
        len(exchange_df),
    )

    upload_status = upload_outputs(
        client,
        bucket,
        prefix,
        final_csv,
        final_parquet,
        exchange_csv,
        final_csv_key,
        final_parquet_key,
        exchange_key,
    )
    if upload_status == "error":
        logger.error("R2 upload failed.")
        return "failed", len(final_df)
    return upload_status, len(final_df)


if __name__ == "__main__":
    ts = int(time.time())
    label = "Salary Optimize"
    pre = discord_user_prefix()
    try:
        result, row_count = main()
        if result == "uploaded":
            send_discord_alert(
                f"✅ {label} — Uploaded at <t:{ts}:f> | rows={row_count}"
            )
        elif result == "no-change":
            send_discord_alert(
                f"✅ {label} — No changes at <t:{ts}:f> | rows={row_count}"
            )
        elif result == "failed":
            send_discord_alert(f"{pre}❌ {label} failed at <t:{ts}:f>")
            sys.exit(1)
        else:
            send_discord_alert(
                f"{pre}⚠️ {label} — status {result} at <t:{ts}:f> | rows={row_count}"
            )
    except Exception:
        logger.exception("Optimizer failed.")
        send_discord_alert(f"{pre}❌ {label} failed at <t:{ts}:f>")
        sys.exit(1)
