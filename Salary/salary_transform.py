"""Salary normalization: period → monthly, FX rates, USD columns."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger("salary.transform")

SALARY_AMOUNT_COLUMNS = [
    "min_salary",
    "max_salary",
    "median_salary",
    "min_base_salary",
    "max_base_salary",
    "median_base_salary",
    "min_additional_pay",
    "max_additional_pay",
    "median_additional_pay",
]

KNOWN_PERIODS = frozenset({"HOUR", "DAY", "WEEK", "MONTH", "YEAR"})
DEFAULT_HOURS_PER_MONTH = 2080.0 / 12.0
DEFAULT_DAYS_PER_MONTH = 5.0 * 52.0 / 12.0
DEFAULT_WEEKS_PER_MONTH = 52.0 / 12.0


@dataclass(frozen=True)
class PeriodFactors:
    hours_per_month: float
    days_per_month: float
    weeks_per_month: float

    def factor_for(self, canonical: str) -> float | None:
        if canonical == "HOUR":
            return self.hours_per_month
        if canonical == "DAY":
            return self.days_per_month
        if canonical == "WEEK":
            return self.weeks_per_month
        if canonical == "MONTH":
            return 1.0
        if canonical == "YEAR":
            return 1.0 / 12.0
        return None


def parse_period(raw: Any) -> str | None:
    """Map API period strings to HOUR|DAY|WEEK|MONTH|YEAR, or None if unknown."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    text = str(raw).strip().upper()
    if not text:
        return None

    if text.endswith("S") and text[:-1] in KNOWN_PERIODS:
        text = text[:-1]

    if text in KNOWN_PERIODS:
        return text

    if "PER HOUR" in text or text in {"HR", "HOURLY"} or "HOUR" in text:
        return "HOUR"
    if text in {"DAILY"} or (text == "DAY"):
        return "DAY"
    if text in {"WEEKLY"} or (text == "WEEK"):
        return "WEEK"
    if text in {"MONTHLY"} or (text == "MONTH"):
        return "MONTH"
    if text in {"YEARLY", "ANNUAL", "ANNUALLY"} or "ANNUAL" in text:
        return "YEAR"

    return None


def period_factors_from_env(
    *,
    hours_per_month: float = DEFAULT_HOURS_PER_MONTH,
    days_per_month: float = DEFAULT_DAYS_PER_MONTH,
    weeks_per_month: float = DEFAULT_WEEKS_PER_MONTH,
) -> PeriodFactors:
    return PeriodFactors(
        hours_per_month=hours_per_month,
        days_per_month=days_per_month,
        weeks_per_month=weeks_per_month,
    )


def normalize_periods(
    df: pd.DataFrame,
    factors: PeriodFactors | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Convert salary amounts to monthly; add audit columns."""
    factors = factors or period_factors_from_env()
    out = df.copy()
    unknown_counts: dict[str, int] = {}

    parsed: list[str] = []
    normalized: list[str | None] = []
    period_factor_vals: list[float | None] = []

    for raw in out.get("salary_period", pd.Series(dtype=object)):
        canonical = parse_period(raw)
        if canonical is None:
            raw_label = str(raw).strip() if raw is not None and not pd.isna(raw) else "<empty>"
            unknown_counts[raw_label] = unknown_counts.get(raw_label, 0) + 1
            parsed.append("UNKNOWN")
            normalized.append(None)
            period_factor_vals.append(None)
            logger.warning("Unrecognized salary_period: %s", raw_label)
            continue

        factor = factors.factor_for(canonical)
        parsed.append(canonical)
        normalized.append("MONTH")
        period_factor_vals.append(factor)

    out["salary_period_parsed"] = parsed
    out["salary_period_normalized"] = normalized
    out["period_factor"] = period_factor_vals

    mask = pd.Series([f is not None for f in period_factor_vals], index=out.index)
    factor_series = pd.Series(
        [f if f is not None else 1.0 for f in period_factor_vals],
        index=out.index,
        dtype=float,
    )
    for col in SALARY_AMOUNT_COLUMNS:
        if col not in out.columns:
            continue
        numeric = pd.to_numeric(out[col], errors="coerce")
        out[col] = numeric.where(~mask, numeric * factor_series)

    if unknown_counts:
        logger.warning("Unknown salary_period summary: %s", unknown_counts)

    return out, unknown_counts


def _date_from_fetched_at(value: Any) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def distinct_fetch_dates(df: pd.DataFrame) -> list[date]:
    if "fetched_at_utc" not in df.columns:
        return [date.today()]
    dates: set[date] = set()
    for val in df["fetched_at_utc"]:
        d = _date_from_fetched_at(val)
        if d:
            dates.add(d)
    return sorted(dates) if dates else [date.today()]


def _env_fx_override(ccy: str) -> float | None:
    key = f"SALARY_FX_USD_{ccy}"
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid manual FX override in env.")
        return None


def _fetch_er_api_fallback(
    session: requests.Session,
    missing: tuple[str, ...],
) -> dict[str, float]:
    """open.er-api.com fallback when Frankfurter/ECB lacks a currency."""
    if not missing:
        return {}
    try:
        resp = session.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        resp.raise_for_status()
        rates = (resp.json() or {}).get("rates") or {}
        return {ccy: float(rates[ccy]) for ccy in missing if ccy in rates}
    except requests.RequestException as exc:
        logger.warning("FX fallback API failed (%s).", type(exc).__name__)
        return {}


def fetch_fx_rates_for_date(
    rate_date: date,
    fx_targets: tuple[str, ...],
    *,
    api_base: str = "https://api.frankfurter.app",
    session: requests.Session | None = None,
) -> dict[str, float]:
    """Return USD→CCY rates (1 USD = X local) for configured targets."""
    session = session or requests.Session()
    base = api_base.rstrip("/")
    date_str = rate_date.isoformat()
    result: dict[str, float] = {}

    for ccy in fx_targets:
        override = _env_fx_override(ccy)
        if override is not None:
            result[ccy] = override

    remaining = [c for c in fx_targets if c not in result]
    if not remaining:
        return result

    for url in (f"{base}/{date_str}", f"{base}/latest"):
        try:
            resp = session.get(
                url,
                params={"from": "USD", "to": ",".join(remaining)},
                timeout=15,
            )
            if resp.status_code == 404 and date_str in url:
                continue
            resp.raise_for_status()
            rates = (resp.json() or {}).get("rates") or {}
            for ccy in remaining:
                if ccy in rates:
                    result[ccy] = float(rates[ccy])
            remaining = [c for c in fx_targets if c not in result]
            if not remaining:
                return result
        except requests.RequestException as exc:
            logger.warning("FX fetch failed (%s).", type(exc).__name__)

    if remaining:
        fallback = _fetch_er_api_fallback(session, tuple(remaining))
        result.update(fallback)
        remaining = [c for c in fx_targets if c not in result]

    if remaining:
        logger.warning(
            "FX rates incomplete for %s (%d of %d currencies missing).",
            date_str,
            len(remaining),
            len(fx_targets),
        )

    return result


def fetch_fx_rates(
    dates: list[date],
    fx_targets: tuple[str, ...],
    *,
    api_base: str = "https://api.frankfurter.app",
) -> dict[date, dict[str, float]]:
    """Cache USD→local rates per date for configured currencies."""
    cache: dict[date, dict[str, float]] = {}
    session = requests.Session()
    for d in dates:
        rates = fetch_fx_rates_for_date(
            d, fx_targets, api_base=api_base, session=session
        )
        if rates:
            cache[d] = rates
        else:
            logger.error("Could not load FX rates for %s.", d.isoformat())
    return cache


def build_exchange_csv(
    rates_by_date: dict[date, dict[str, float]],
    fx_targets: tuple[str, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for rate_date in sorted(rates_by_date):
        usd_to = rates_by_date[rate_date]
        for ccy in fx_targets:
            if ccy not in usd_to:
                continue
            rate = usd_to[ccy]
            rows.append(
                {
                    "rate_date": rate_date.isoformat(),
                    "from_currency": "USD",
                    "to_currency": ccy,
                    "rate": rate,
                }
            )
            rows.append(
                {
                    "rate_date": rate_date.isoformat(),
                    "from_currency": ccy,
                    "to_currency": "USD",
                    "rate": 1.0 / rate if rate else None,
                }
            )
    return pd.DataFrame(rows, columns=["rate_date", "from_currency", "to_currency", "rate"])


def _usd_rate_for_row(
    currency: Any,
    fetch_date: date | None,
    rates_by_date: dict[date, dict[str, float]],
) -> float | None:
    if currency is None or (isinstance(currency, float) and pd.isna(currency)):
        return None
    ccy = str(currency).strip().upper()
    if ccy == "USD":
        return 1.0
    if not fetch_date or fetch_date not in rates_by_date:
        return None
    usd_to = rates_by_date[fetch_date]
    if ccy in usd_to and usd_to[ccy]:
        return usd_to[ccy]
    return None


def apply_usd_columns(
    df: pd.DataFrame,
    rates_by_date: dict[date, dict[str, float]],
) -> pd.DataFrame:
    """Add *_usd columns using monthly native amounts."""
    out = df.copy()
    fetch_dates = [
        _date_from_fetched_at(v) for v in out.get("fetched_at_utc", pd.Series(dtype=object))
    ]

    for col in SALARY_AMOUNT_COLUMNS:
        if col not in out.columns:
            continue
        usd_col = f"{col}_usd"
        usd_vals: list[float | None] = []
        numeric = pd.to_numeric(out[col], errors="coerce")
        currencies = out.get("salary_currency", pd.Series([None] * len(out)))
        for idx in range(len(out)):
            amount = numeric.iloc[idx]
            if pd.isna(amount):
                usd_vals.append(None)
                continue
            rate_usd_to_ccy = _usd_rate_for_row(
                currencies.iloc[idx] if idx < len(currencies) else None,
                fetch_dates[idx] if idx < len(fetch_dates) else None,
                rates_by_date,
            )
            if rate_usd_to_ccy is None:
                usd_vals.append(None)
            elif rate_usd_to_ccy == 1.0:
                usd_vals.append(float(amount))
            else:
                usd_vals.append(float(amount) / rate_usd_to_ccy)
        out[usd_col] = usd_vals

    return out
