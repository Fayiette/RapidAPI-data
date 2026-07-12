# Currency Rates Pipeline

Public GitHub Actions job that captures **USD-base** foreign exchange rates once per UTC calendar month and stores them in Cloudflare R2 as CSV + Parquet.

**Base currency:** `USD` (fixed in code).

**Extra currencies:** configured only via the `EXTRA_CURRENCIES` secret (never committed to this repo).

## What it does

1. Pulls the existing rates CSV from R2 (or bootstraps empty on first run).
2. Checks whether the current UTC `YYYY-MM` month is already captured — if yes, exits successfully (dedup for flaky reruns).
3. Calls [ExchangeRate-API](https://www.exchangerate-api.com/) (`/latest/USD`).
4. Appends one row per target currency (USD + extras from secret).
5. Writes CSV + Parquet locally, uploads to R2 only if the CSV hash changed.



## Data schema


| Column                     | Description                                   |
| -------------------------- | --------------------------------------------- |
| `year_month`               | UTC month bucket (`YYYY-MM`)                  |
| `base_currency`            | Always `USD`                                  |
| `target_currency`          | ISO 4217 code                                 |
| `units_per_usd`            | How many units of target currency equal 1 USD |
| `fetched_at_utc`           | ISO8601 snapshot time                         |
| `api_time_last_update_utc` | API `time_last_update_utc`                    |


**Multi-hop conversion (via USD):**

- Target → USD: `amount / units_per_usd[target]`
- Target A → Target B: `amount / units_per_usd[A] * units_per_usd[B]`



## GitHub setup

1. Create a **public** repo from this project.
2. In **Settings → Environments →** `prod` **→ Environment secrets**, set:


| Secret                          | Required |
| ------------------------------- | -------- |
| `EXTRA_CURRENCIES`              | Yes      |
| `EXCHANGERATE_API_KEY`          | Yes      |
| `R2_BUCKET`                     | Yes      |
| `R2_ACCESS_KEY_ID`              | Yes      |
| `R2_SECRET_ACCESS_KEY`          | Yes      |
| `R2_ENDPOINT`                   | Yes      |
| `CURRENCY_RATES_CSV_PATH`       | Yes      |
| `CURRENCY_RATES_PARQUET_PATH`   | Yes      |
| `R2_CURRENCY_RATES_CSV_KEY`     | Yes      |
| `R2_CURRENCY_RATES_PARQUET_KEY` | Yes      |
| `DISCORD_WEBHOOK_URL`           | No       |
| `DISCORD_USER_ID`               | No       |


Do **not** put secret values in the repo, issues, or workflow YAML.

1. Workflow runs on the 1st of each month (06:00 UTC) and via **Actions → Fetch Currency Rates → Run workflow**.



## Local development

```powershell
pip install -r requirements.txt
copy .env.example dev.env
# Fill dev.env locally (gitignored), then:
python currency_rates.py
```



