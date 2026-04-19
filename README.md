# API_data — Product ETL (Pirpos API → PySpark → CSV)

Small Python pipeline that **pulls product data from the Pirpos REST API**, **cleans and reshapes it with Apache Spark**, and **writes a single consolidated CSV** for downstream use (reporting, analysis, or imports).

## What this project does

1. **Authenticate** against the API and request the `/products` endpoint.
2. **Normalize** nested JSON fields (objects and arrays are serialized to strings) so Spark can infer a stable schema.
3. **Load** the records into a Spark `DataFrame` and run a **cleansing** pass:
   - Turn empty strings into SQL `NULL` on string columns.
   - Parse `createdOn` and `modifiedOn` as timestamps.
   - Drop columns that are entirely null.
   - Standardize product names (trim, title case, small typo corrections).
   - Derive **price** from the first entry in `locationsStock` and **category name** from nested `category` JSON.
   - Keep only active, non-deleted products.
   - Deduplicate by product **name**, keeping the row with the latest **modifiedOn**.
4. **Export** the result to `output/products_clean.csv` (header included).

The main script is `file.py`; authentication and headers are provided by a local `credentials.py` file that is **not** tracked in Git (see [Setup](#setup)).

## What was accomplished

- End-to-end **API → Spark → CSV** flow with explicit error handling on HTTP calls (`raise_for_status`).
- **Schema-safe ingestion** of semi-structured API payloads (nested dicts/lists serialized before `createDataFrame`).
- **Data quality rules** aligned with business logic: active products only, deduplication by name, derived price and category, null and date handling.
- **Reproducible output** under `output/` as a single CSV file suitable for sharing or loading into other tools.

## Requirements

- **Python 3** (3.8+ recommended)
- **Apache Spark** with PySpark installed and a compatible **Java** runtime (Spark’s usual prerequisites)
- **requests**

Example install (adjust for your environment):

```bash
pip install requests pyspark
```

## Setup

1. Clone the repository.

2. Create a **`credentials.py`** in the project root (this file is listed in `.gitignore`). It should expose:

   - `BASE_URL` — API base URL (e.g. `https://api.pirpos.com`)
   - `headers` — a `dict` with `Authorization: Bearer <token>` (and any other headers the API expects)

   The included project pattern logs in via `/login`, obtains `tokenCurrent`, and builds `headers` with `auth_headers(token)`. **Do not commit real emails, passwords, or tokens**; prefer environment variables or a secrets manager for production.

3. Ensure an **`output/`** directory exists if your Spark write path requires it (the script writes to `output/products_clean.csv`).

## How to run

From the project root:

```bash
python file.py
```

On success you should see Spark output, cleanse logs, and a message that the CSV was written to **`output/products_clean.csv`**.

## Project layout

| Path | Role |
|------|------|
| `file.py` | API fetch, Spark session, normalization, cleansing, CSV export |
| `credentials.py` | API base URL and authenticated headers (local only; gitignored) |
| `output/` | Generated CSV (gitignored) |

## Security note

Treat API credentials like production secrets: keep them out of version control, rotate them if they were ever exposed, and use environment variables or a vault when you move beyond local runs.
