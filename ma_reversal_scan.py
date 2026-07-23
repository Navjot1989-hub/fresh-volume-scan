#!/usr/bin/env python3
"""
52-Week Reversal Scan — NSE stocks that corrected ~50% off their 52-week high
and have now reclaimed BOTH their 10-month and 12-month moving averages.

Universe: whole NSE cash market (EQ series), market cap >= Rs 5,000 cr.

Pipeline:
  1. nse_session() + fetch_bhavcopy() — one NSE "security-wise full bhavcopy"
     CSV for the latest trading day, purely to get the full EQ symbol
     universe (NSE has no clean public "list all equities" endpoint that's
     lighter than this).
  2. fetch_history() — per symbol, ~2 years of daily closes from Yahoo
     Finance's public chart endpoint (SYMBOL.NS), fetched concurrently.
  3. analyse() — per symbol: 52-week high, the trough reached after that high
     (the "corrected nearly 50%" leg), current 10-month/12-month SMA, and
     whether price has *freshly* reclaimed both averages (was below at least
     one of them within the last CROSS_LOOKBACK_DAYS sessions, is above both
     now).
  4. market-cap gate — for the shortlist only, confirm market cap >= the
     floor via each company's public screener.in page (screener's custom
     screens need login; the public per-company page doesn't).
  5. report — ranked by correction depth.

Technical/quantitative screen, NOT investment advice. A stock reclaiming its
long-term averages after a deep correction is a "turning bullish" *signal*,
not a guarantee — many such reclaims fail and roll over again. Verify
fundamentals and the chart before acting.
"""

import os
import sys
import csv
import io
import time
import datetime
import concurrent.futures as cf
from statistics import mean

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Config (env-overridable)
# --------------------------------------------------------------------------- #
MCAP_MIN = float(os.environ.get("MCAP_MIN", 5000))            # Rs cr floor
CORRECTION_MIN = float(os.environ.get("CORRECTION_MIN", 40))  # % drawdown band
CORRECTION_MAX = float(os.environ.get("CORRECTION_MAX", 60))  # ("nearly 50%")
SMA_10M_DAYS = int(os.environ.get("SMA_10M_DAYS", 210))       # ~10 trading months
SMA_12M_DAYS = int(os.environ.get("SMA_12M_DAYS", 252))       # ~12 trading months / 52 weeks
CROSS_LOOKBACK_DAYS = int(os.environ.get("CROSS_LOOKBACK_DAYS", 10))  # "fresh" cross window
YF_RANGE = os.environ.get("YF_RANGE", "2y")                   # history pulled per symbol
MAX_LOOKUP = int(os.environ.get("MAX_LOOKUP", 200))            # cap screener mcap lookups
PAGE_DELAY = float(os.environ.get("PAGE_DELAY", 0.4))          # polite delay: screener.in
YF_WORKERS = int(os.environ.get("YF_WORKERS", 10))             # concurrent Yahoo fetches
MAX_CALENDAR_BACK = int(os.environ.get("MAX_CALENDAR_BACK", 15))  # bhavcopy holiday-safe walk-back
MAX_SYMBOLS = int(os.environ.get("MAX_SYMBOLS", 0))            # 0 = no cap (debug/smoke-test aid)
NSE_PROXY_URL = os.environ.get("NSE_PROXY_URL", "").strip()     # e.g. http://user:pass@host:port

NEEDED_DAYS = SMA_12M_DAYS + CROSS_LOOKBACK_DAYS

BHAV_URL = ("https://archives.nseindia.com/products/content/"
            "sec_bhavdata_full_{ddmmyyyy}.csv")
NSE_HOME = "https://www.nseindia.com"
YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}.NS"
YF_HOME = "https://finance.yahoo.com"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}


# --------------------------------------------------------------------------- #
# NSE bhavcopy — universe of EQ symbols
# --------------------------------------------------------------------------- #
def nse_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    if NSE_PROXY_URL:
        s.proxies.update({"http": NSE_PROXY_URL, "https": NSE_PROXY_URL})
    try:
        s.get(NSE_HOME, timeout=20)
    except requests.RequestException:
        pass
    return s


def fetch_bhavcopy(session, d, retries=3):
    """Return {SYMBOL: close} for SERIES==EQ on date d, or None if unavailable."""
    url = BHAV_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, timeout=30)
        except requests.RequestException as e:
            print(f"  {d:%Y-%m-%d}: fetch error ({e}), attempt {attempt}/{retries}")
            r = None
        else:
            if r.status_code == 404:
                return None
            if r.status_code == 200 and "SYMBOL" in r.text[:200]:
                break
            if attempt == 1:
                print(f"  {d:%Y-%m-%d}: status {r.status_code}, "
                      f"content-type {r.headers.get('Content-Type')!r}")
        if attempt < retries:
            time.sleep(2 * attempt)
            try:
                session.get(NSE_HOME, timeout=20)
            except requests.RequestException:
                pass
    else:
        return None
    out = {}
    reader = csv.DictReader(io.StringIO(r.text))
    for raw in reader:
        row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
        if row.get("SERIES") != "EQ":
            continue
        sym = row.get("SYMBOL")
        if sym:
            out[sym] = row.get("CLOSE_PRICE")
    return out


def fetch_universe(session):
    """Walk back from yesterday until a usable bhavcopy is found."""
    d = datetime.date.today() - datetime.timedelta(days=1)
    tries = 0
    while tries < MAX_CALENDAR_BACK:
        tries += 1
        if d.weekday() < 5:
            day = fetch_bhavcopy(session, d)
            if day:
                print(f"  Universe from {d:%Y-%m-%d}: {len(day)} EQ symbols")
                return sorted(day.keys())
            time.sleep(0.3)
        d -= datetime.timedelta(days=1)
    return []


# --------------------------------------------------------------------------- #
# Yahoo Finance history
# --------------------------------------------------------------------------- #
def yahoo_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        s.get(YF_HOME, timeout=15)
    except requests.RequestException:
        pass
    return s


def fetch_history(session, symbol, retries=2):
    """Return an ascending-by-date list of daily closes, or None."""
    url = YF_CHART_URL.format(sym=requests.utils.quote(symbol, safe=""))
    params = {"range": YF_RANGE, "interval": "1d"}
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=20)
        except requests.RequestException:
            time.sleep(1.5 * attempt)
            continue
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError:
                return None
            result = (data.get("chart") or {}).get("result")
            if not result:
                return None
            res = result[0]
            ts = res.get("timestamp") or []
            quotes = (res.get("indicators") or {}).get("quote") or [{}]
            closes = quotes[0].get("close") or []
            pairs = [(t, c) for t, c in zip(ts, closes) if c is not None]
            if len(pairs) < NEEDED_DAYS:
                return None
            pairs.sort(key=lambda p: p[0])
            return [c for _, c in pairs]
        if r.status_code in (429, 999) and attempt < retries:
            time.sleep(2 * attempt)
            continue
        return None
    return None


# --------------------------------------------------------------------------- #
# Correction + moving-average-reclaim signal
# --------------------------------------------------------------------------- #
def analyse(symbol, closes):
    n = len(closes)
    if n < NEEDED_DAYS:
        return None

    window = closes[-SMA_12M_DAYS:]
    high_val = max(window)
    if high_val <= 0:
        return None
    high_idx = window.index(high_val)          # first (earliest) occurrence of the high
    after_high = window[high_idx:]
    trough = min(after_high)
    drawdown = (high_val - trough) / high_val * 100
    if not (CORRECTION_MIN <= drawdown <= CORRECTION_MAX):
        return None

    current = closes[-1]
    sma10_now = mean(closes[-SMA_10M_DAYS:])
    sma12_now = mean(closes[-SMA_12M_DAYS:])
    if not (current > sma10_now and current > sma12_now):
        return None

    days_since_cross = None
    for t in range(1, CROSS_LOOKBACK_DAYS + 1):
        idx = n - 1 - t
        if idx - SMA_12M_DAYS + 1 < 0:
            break
        price_t = closes[idx]
        sma10_t = mean(closes[idx - SMA_10M_DAYS + 1: idx + 1])
        sma12_t = mean(closes[idx - SMA_12M_DAYS + 1: idx + 1])
        if price_t <= sma10_t or price_t <= sma12_t:
            days_since_cross = t
            break
    if days_since_cross is None:
        return None   # already above both averages throughout the lookback window — not a fresh cross

    return {
        "ticker": symbol,
        "high": round(high_val, 2),
        "trough": round(trough, 2),
        "drawdown": round(drawdown, 1),
        "price": round(current, 2),
        "sma10": round(sma10_now, 1),
        "sma12": round(sma12_now, 1),
        "days_since_cross": days_since_cross,
        "upside_to_high": round((high_val / current - 1) * 100, 1),
    }


def scan_universe(symbols):
    session = yahoo_session()
    candidates = []
    fetched = failed = 0
    with cf.ThreadPoolExecutor(max_workers=YF_WORKERS) as ex:
        futures = {ex.submit(fetch_history, session, sym): sym for sym in symbols}
        for i, fut in enumerate(cf.as_completed(futures), 1):
            sym = futures[fut]
            try:
                closes = fut.result()
            except Exception:
                closes = None
            if closes is None:
                failed += 1
            else:
                fetched += 1
                m = analyse(sym, closes)
                if m:
                    candidates.append(m)
            if i % 250 == 0:
                print(f"  ...{i}/{len(symbols)} symbols processed "
                      f"({fetched} ok, {failed} failed, {len(candidates)} candidates so far)")
    print(f"  Done: {fetched} symbols with usable history, {failed} failed/skipped, "
          f"{len(candidates)} pass the correction+reclaim filter.")
    return candidates


# --------------------------------------------------------------------------- #
# screener.in enrichment (market cap + name, shortlist only)
# --------------------------------------------------------------------------- #
def _to_float(text):
    if text is None:
        return None
    import re
    m = re.search(r"-?\d[\d,]*\.?\d*", str(text).replace(",", ""))
    return float(m.group()) if m else None


def fetch_screener(ticker):
    last_url = ""
    for path in (f"/company/{ticker}/consolidated/", f"/company/{ticker}/"):
        last_url = "https://www.screener.in" + path
        try:
            r = requests.get(last_url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            continue
        if r.status_code == 200 and "Compounded" in r.text:
            return last_url, BeautifulSoup(r.text, "html.parser")
    return last_url, None


def parse_top_ratios(soup):
    out = {}
    ul = soup.find(id="top-ratios")
    if not ul:
        return out
    for li in ul.find_all("li"):
        name = li.find(class_="name")
        if not name:
            continue
        nums = [_to_float(x.get_text()) for x in li.find_all(class_="number")]
        out[name.get_text(strip=True)] = [x for x in nums if x is not None]
    return out


def enrich_screener(m):
    url, soup = fetch_screener(m["ticker"])
    m["url"] = url
    if soup is None:
        m["mcap"], m["name"] = None, m["ticker"]
        return m
    top = parse_top_ratios(soup)
    mc = top.get("Market Cap")
    name_tag = soup.find("h1")
    m["mcap"] = mc[0] if mc else None
    m["name"] = name_tag.get_text(strip=True) if name_tag else m["ticker"]
    return m


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def fmt(v, suffix=""):
    return f"{v:g}{suffix}" if isinstance(v, (int, float)) else "n/a"


def build_report(rows, today, scanned_syms, candidates_count):
    lines = []
    lines.append("# 52-Week Reversal Scan — corrected ~50%, reclaiming 10M & 12M averages\n")
    lines.append(
        f"**Date:** {today} · **Universe:** NSE EQ, market cap ≥ Rs {MCAP_MIN:g} cr · "
        f"**Signal:** drawdown from 52-week high of {CORRECTION_MIN:g}%–{CORRECTION_MAX:g}%, "
        f"price now above both its 10-month and 12-month moving averages, having been below "
        f"at least one within the last {CROSS_LOOKBACK_DAYS} trading days · "
        "**Source:** NSE bhavcopy (universe) + Yahoo Finance (price history) + screener.in (market cap)\n")
    lines.append(
        "> Technical/quantitative screen, **NOT investment advice**. Reclaiming long-term "
        "moving averages after a deep correction is a *signal*, not a guarantee — many such "
        "reclaims fail and roll over again. Verify fundamentals, the reason for the original "
        "correction, and the chart before acting.\n")
    lines.append(
        f"Scanned **{scanned_syms}** NSE EQ symbols; **{candidates_count}** matched the "
        f"correction + fresh moving-average reclaim pattern; **{len(rows)}** are ≥ Rs "
        f"{MCAP_MIN:g} cr market cap.\n")

    if not rows:
        lines.append("_No names cleared the filter this run._")
        return "\n".join(lines)

    lines.append("## Ranked by correction depth\n")
    lines.append("| # | Company | Ticker | Mcap (cr) | 52W High | Trough | Correction % | "
                 "Price | 10M SMA | 12M SMA | Days since cross | Upside to 52W high |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(rows, 1):
        lines.append(
            f"| {i} | {r['name']} | {r['ticker']} | {fmt(r['mcap'])} | {fmt(r['high'])} | "
            f"{fmt(r['trough'])} | **{r['drawdown']}%** | {fmt(r['price'])} | "
            f"{fmt(r['sma10'])} | {fmt(r['sma12'])} | {r['days_since_cross']} | "
            f"{fmt(r['upside_to_high'], '%')} |")

    lines.append("\n## Sources\n")
    for r in rows:
        lines.append(f"- [{r['ticker']}]({r['url']})")
    lines.append(
        f"\n*Correction % = (52-week high − post-high trough) ÷ 52-week high. "
        f"10M/12M SMA = simple moving average of daily close over the last {SMA_10M_DAYS}/"
        f"{SMA_12M_DAYS} trading sessions. 'Days since cross' = trading sessions since price "
        "was last at/below either average. Market cap confirmed via screener.in. "
        "Not investment advice.*")
    return "\n".join(lines)


def build_email_html(rows, today):
    head = (
        '<div style="font-family:Arial,sans-serif;color:#1a1a1a;max-width:1000px">'
        f'<h2>52-Week Reversal Scan ({today})</h2>'
        '<p style="background:#fff8e1;border-left:4px solid #f0ad4e;padding:8px 12px;'
        f'font-size:13px">Mcap ≥ Rs {MCAP_MIN:g} cr, corrected {CORRECTION_MIN:g}–'
        f'{CORRECTION_MAX:g}% off the 52-week high, now freshly above both the 10-month '
        'and 12-month averages. <b>Not investment advice.</b></p>')
    if not rows:
        return head + "<p>No names cleared the filter this run.</p></div>"
    head += (
        '<table cellpadding="6" cellspacing="0" border="1" '
        'style="border-collapse:collapse;font-size:13px">'
        '<tr style="background:#1f3a5f;color:#fff"><th>#</th><th>Company</th>'
        '<th>Ticker</th><th>Mcap</th><th>Correction</th><th>Price</th>'
        '<th>10M SMA</th><th>12M SMA</th><th>Days since cross</th></tr>')
    body = ""
    for i, r in enumerate(rows, 1):
        body += (f"<tr><td>{i}</td><td>{r['name']}</td><td>{r['ticker']}</td>"
                 f"<td>{fmt(r['mcap'])}</td><td><b>{r['drawdown']}%</b></td>"
                 f"<td>{fmt(r['price'])}</td><td>{fmt(r['sma10'])}</td>"
                 f"<td>{fmt(r['sma12'])}</td><td>{r['days_since_cross']}</td></tr>")
    return head + body + "</table></div>"


def send_email(subject, html):
    user = os.environ.get("SMTP_USER")
    pw = os.environ.get("SMTP_PASS")
    to = os.environ.get("EMAIL_TO")
    if not (user and pw and to):
        print("Email not configured (SMTP_USER/SMTP_PASS/EMAIL_TO) — skipping.")
        return
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(user, pw)
        s.sendmail(user, [to], msg.as_string())
    print(f"Email sent to {to}")


# --------------------------------------------------------------------------- #
def main():
    today = datetime.date.today().isoformat()

    print("Fetching NSE EQ universe from bhavcopy...")
    session = nse_session()
    symbols = fetch_universe(session)
    if not symbols:
        print("Could not get an NSE EQ universe (bhavcopy blocked/unavailable). Aborting.")
        sys.exit(1)
    if MAX_SYMBOLS:
        symbols = symbols[:MAX_SYMBOLS]
        print(f"  MAX_SYMBOLS set — limiting to first {len(symbols)} symbols (smoke test).")

    print(f"Pulling {YF_RANGE} of daily history per symbol from Yahoo Finance "
          f"({len(symbols)} symbols, {YF_WORKERS} concurrent workers)...")
    candidates = scan_universe(symbols)
    candidates.sort(key=lambda m: m["drawdown"], reverse=True)

    print(f"Confirming market cap (top {min(len(candidates), MAX_LOOKUP)} via screener.in)...")
    rows = []
    for m in candidates[:MAX_LOOKUP]:
        enrich_screener(m)
        if m.get("mcap") is not None and m["mcap"] >= MCAP_MIN:
            rows.append(m)
            print(f"  ✓ {m['ticker']}: {m['drawdown']}% correction, mcap {m['mcap']:g} cr, "
                  f"cross {m['days_since_cross']}d ago")
        time.sleep(PAGE_DELAY)

    rows.sort(key=lambda r: r["drawdown"], reverse=True)
    report = build_report(rows, today, len(symbols), len(candidates))

    reports_dir = os.path.join(os.path.dirname(__file__), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    for path in (os.path.join(reports_dir, f"ma-reversal-{today}.md"),
                 os.path.join(reports_dir, "ma-reversal-latest.md")):
        with open(path, "w", encoding="utf-8") as f:
            f.write(report)
    print(f"Wrote reports/ma-reversal-{today}.md ({len(rows)} names)")

    subject = f"52-Week Reversal Scan ({today}): {len(rows)} names"
    if rows:
        subject = (f"52-Week Reversal Scan ({today}): {rows[0]['name']} "
                   f"-{rows[0]['drawdown']}% ({len(rows)} names)")
    send_email(subject, build_email_html(rows, today))


if __name__ == "__main__":
    main()
