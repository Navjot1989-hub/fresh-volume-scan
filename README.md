# Fresh Volume Scan

NSE volume-surge scanner for **large-caps only (market cap ≥ Rs 10,000 cr)**,
liquidity-gated, with two additions beyond a plain volume screen:

- **Price target** — a plain technical projection off the stock's 52-week range,
  not an analyst estimate.
- **Likely reason for surge** — a best-effort recent-headline lookup, not
  confirmed causation.

Analytical research, **NOT investment advice**.

## What it scans

1. Downloads the last ~25 trading days of NSE "security-wise full bhavcopy" CSVs
   (`archives.nseindia.com`) — bulk EQ-series volume, turnover, close price,
   delivery %.
2. For every symbol, computes **5-day avg volume ÷ prior 20-day avg volume**
   (the "surge ratio") and a **liquidity floor** (avg turnover ≥ Rs 20 cr/day
   by default).
3. For names clearing both filters, reads the public screener.in page to
   confirm **market cap ≥ Rs 10,000 cr** and pull the 52-week high/low.
4. Computes a **price target**:
   - If the price is still below its 52-week high, the target is that high —
     the nearest unbroken overhead resistance.
   - If the price is already at/above its 52-week high (a fresh breakout),
     the target is a classic **measured move**: current price + 50% of the
     52-week range.
5. Looks up a **likely reason for the surge** via a free Google News RSS
   search (`<company> share`, last `NEWS_LOOKBACK_DAYS` days) and reports the
   most recent headline, or "No recent news found" if nothing turns up —
   volume can move without any news at all.
6. Writes `reports/volume-<date>.md` and `reports/volume-latest.md`, and
   optionally emails a summary.

## Config (env vars, all optional)

| Var | Default | Meaning |
|---|---|---|
| `MCAP_MIN` | `10000` | Market-cap floor, Rs cr |
| `VOL_SURGE_MIN` | `1.5` | Recent/base avg-volume ratio to qualify |
| `RECENT_DAYS` | `5` | "Recent" window |
| `BASE_DAYS` | `20` | Baseline window |
| `MIN_TURNOVER_LACS` | `2000` | Liquidity floor, Rs lakh/day (~Rs 20 cr) |
| `MAX_LOOKUP` | `200` | Cap on screener.in lookups per run |
| `PAGE_DELAY` | `0.5` | Delay between screener/news fetches |
| `NEWS_LOOKBACK_DAYS` | `3` | Recency window for the surge-reason headline |
| `NSE_PROXY_URL` | — | Proxy for NSE requests — see below |

## Running it

- **Manual only** — the **Run workflow** button on the **Actions** tab.
  This repo does *not* run on a schedule.
- **Locally:** `pip install -r requirements.txt && python volume_scan.py`

## Note on data access — NSE's Akamai block

NSE's CDN (Akamai) returns a **503** to requests from GitHub-hosted Actions
runners' datacenter IPs, and — confirmed by direct testing — also to plain
`requests`/`curl` calls and even a real headless-Chromium browser from a
genuine residential IP. This is **client-fingerprint-based bot detection**,
not a simple IP blocklist, so plain retries or IP rotation alone won't get
past it.

The script supports routing NSE requests through a proxy via the
`NSE_PROXY_URL` repo secret (`http://user:pass@host:port`), but a plain
residential/rotating-IP proxy is not guaranteed to work given the
fingerprinting above — an anti-bot-aware proxy/unlocker service (e.g. one
designed to defeat Akamai/Cloudflare bot managers specifically) is more
likely to. That tradeoff is a deliberate choice for whoever runs this repo
to make, not baked into the code.

Until that's resolved, expect scheduled/cloud runs to fail; running from a
machine/network that already has working NSE access (e.g. via browser) is
the reliable path today.
