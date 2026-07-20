#!/usr/bin/env python3
"""
Fresh Volume Scan — NSE large-caps (>= Rs 10,000 cr) with a recent volume build-up.

Scans the whole NSE cash market for liquid large-caps whose **average volume over
the last ~5 trading days is well above their prior ~20-day average** — a volume
build-up / accumulation signal.

Pipeline:
  1. collect_history()   — download the last ~25 NSE "security-wise full bhavcopy"
     CSVs (one per trading day, ~2,000 symbols each) — bulk volume in a few files.
  2. compute()            — per EQ symbol: 5-day avg volume vs the prior 20-day avg
     (the "surge ratio"), recent turnover (a liquidity floor), and 5-day price move.
  3. market-cap gate      — for the surging, liquid shortlist only, read each
     company's PUBLIC screener.in page to confirm market cap >= the floor.
  4. price target         — technical heuristic from the 52-week range (see
     price_target() below) — NOT an analyst estimate.
  5. surge reason         — best-effort recent-headline lookup via Google News,
     for context only — not a confirmed cause of the move.
  6. report + email       — ranked by surge ratio.

screener.in's custom *screens* need a login, so market cap/52w-range is confirmed
per company on public pages (cheap, because the bhavcopy volume filter runs first).

Analytical research, NOT investment advice. A volume spike is direction-agnostic
— it can precede a breakdown as easily as a breakout. The price target is a plain
technical projection (52-week range), not a forecast. The surge reason is a
best-effort recent-headline match, not confirmed causation. Verify before acting.
"""

import os
import re
import sys
import csv
import io
import time
import datetime
import xml.etree.ElementTree as ET
from statistics import mean
from email.mime.text import MIMEText
import smtplib

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Config (env-overridable so the workflow / user can tune without code edits)
# --------------------------------------------------------------------------- #
MCAP_MIN = float(os.environ.get("MCAP_MIN", 10000))        # Rs cr — market-cap floor
VOL_SURGE_MIN = float(os.environ.get("VOL_SURGE_MIN", 1.5))  # recent/base avg-vol ratio
RECENT_DAYS = int(os.environ.get("RECENT_DAYS", 5))        # the "last 4-5 days" window
BASE_DAYS = int(os.environ.get("BASE_DAYS", 20))           # prior baseline window
MIN_TURNOVER_LACS = float(os.environ.get("MIN_TURNOVER_LACS", 2000))  # ~Rs 20 cr/day liquidity floor
MAX_LOOKUP = int(os.environ.get("MAX_LOOKUP", 200))        # cap screener mcap lookups
PAGE_DELAY = float(os.environ.get("PAGE_DELAY", 0.5))      # polite delay between fetches
MAX_CALENDAR_BACK = int(os.environ.get("MAX_CALENDAR_BACK", 50))  # holiday-safe scan back
NEWS_LOOKBACK_DAYS = int(os.environ.get("NEWS_LOOKBACK_DAYS", 3))  # recent-headline window
NSE_PROXY_URL = os.environ.get("NSE_PROXY_URL", "").strip()  # e.g. http://user:pass@host:port

NEEDED = RECENT_DAYS + BASE_DAYS

BHAV_URL = ("https://archives.nseindia.com/products/content/"
            "sec_bhavdata_full_{ddmmyyyy}.csv")
NSE_HOME = "https://www.nseindia.com"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


# --------------------------------------------------------------------------- #
# NSE bhavcopy download
# --------------------------------------------------------------------------- #
def nse_session():
    """A requests session warmed with NSE cookies (best-effort; NSE can gate
    datacenter IPs). Routes through NSE_PROXY_URL if set — GitHub-hosted
    runners sit on datacenter IP ranges that NSE's CDN (Akamai) 503s outright,
    independent of retries, so a residential/rotating proxy is the actual fix.
    (Confirmed by direct testing: the block is client/IP based for datacenter
    ranges — see README for the full diagnostic writeup.)"""
    s = requests.Session()
    s.headers.update(HEADERS)
    if NSE_PROXY_URL:
        s.proxies.update({"http": NSE_PROXY_URL, "https": NSE_PROXY_URL})
    try:
        s.get(NSE_HOME, timeout=20)
    except requests.RequestException:
        pass
    return s


_diag_printed = False   # print one full diagnostic snippet per run, on the first miss


def fetch_bhavcopy(session, d, retries=3):
    """Return {SYMBOL: row} for SERIES==EQ on date d, or None if no file.

    Retries transient failures (NSE rate-limiting / momentary WAF blocks) with
    backoff, re-warming cookies before each retry. A clean 404 (no file for
    that date — holiday) is not retried."""
    global _diag_printed
    url = BHAV_URL.format(ddmmyyyy=d.strftime("%d%m%Y"))
    last_status = "error"
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
            last_status = r.status_code
            if not _diag_printed:
                _diag_printed = True
                print(f"  {d:%Y-%m-%d}: diagnostic — status {r.status_code}, "
                      f"content-type {r.headers.get('Content-Type')!r}, "
                      f"body[:200]={r.text[:200]!r}")
        if attempt < retries:
            time.sleep(2 * attempt)
            try:
                session.get(NSE_HOME, timeout=20)     # re-warm cookies before retry
            except requests.RequestException:
                pass
    else:
        print(f"  {d:%Y-%m-%d}: giving up after {retries} attempts (last status: {last_status})")
        return None
    out = {}
    reader = csv.DictReader(io.StringIO(r.text))
    for raw in reader:
        # NSE pads fields/headers with spaces — strip everything.
        row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
        if row.get("SERIES") != "EQ":
            continue
        sym = row.get("SYMBOL")
        try:
            out[sym] = {
                "vol": float(row["TTL_TRD_QNTY"]),
                "turnover": float(row["TURNOVER_LACS"]),
                "close": float(row["CLOSE_PRICE"]),
                "deliv": float(row["DELIV_PER"]) if row.get("DELIV_PER") not in
                ("", "-", None) else None,
            }
        except (KeyError, ValueError):
            continue
    return out


def collect_history(session, needed):
    """Walk back from yesterday, collecting `needed` trading days of bhavcopy.
    Returns list oldest->newest of (date, {symbol: row})."""
    history = []
    d = datetime.date.today() - datetime.timedelta(days=1)
    tries = 0
    while len(history) < needed and tries < MAX_CALENDAR_BACK:
        tries += 1
        if d.weekday() < 5:                       # skip Sat/Sun outright
            day = fetch_bhavcopy(session, d)
            if day:
                history.append((d, day))
                print(f"  {d:%Y-%m-%d}: {len(day)} EQ symbols "
                      f"({len(history)}/{needed})")
            time.sleep(0.3)
        d -= datetime.timedelta(days=1)
    history.reverse()
    return history


# --------------------------------------------------------------------------- #
# Volume-surge metrics
# --------------------------------------------------------------------------- #
def build_series(history):
    """Pivot history into {symbol: {vol:[...], close:[...], turnover:[...],
    deliv: latest}} for symbols present on every collected day."""
    n = len(history)
    syms = set(history[0][1])
    for _, day in history[1:]:
        syms &= set(day)                          # keep names present all days
    series = {}
    for s in syms:
        vol, close, turn = [], [], []
        for _, day in history:
            r = day[s]
            vol.append(r["vol"]); close.append(r["close"]); turn.append(r["turnover"])
        series[s] = {"vol": vol, "close": close, "turnover": turn,
                     "deliv": history[-1][1][s]["deliv"], "days": n}
    return series


def compute(sym, d):
    """Return surge metrics for one symbol, or None if not enough data."""
    vol, close, turn = d["vol"], d["close"], d["turnover"]
    if len(vol) < NEEDED:
        return None
    recent = vol[-RECENT_DAYS:]
    base = vol[-(NEEDED):-RECENT_DAYS]            # the BASE_DAYS before the recent window
    recent_avg, base_avg = mean(recent), mean(base)
    if base_avg <= 0:
        return None
    recent_turnover = mean(turn[-RECENT_DAYS:])
    price_chg = ((close[-1] / close[-(RECENT_DAYS + 1)] - 1) * 100
                 if close[-(RECENT_DAYS + 1)] else None)
    return {
        "ticker": sym,
        "surge": round(recent_avg / base_avg, 2),
        "recent_avg_vol": recent_avg,
        "base_avg_vol": base_avg,
        "recent_turnover_lacs": recent_turnover,
        "price_chg_5d": round(price_chg, 1) if price_chg is not None else None,
        "price": close[-1],
        "deliv": d["deliv"],
    }


def qualifies_volume(m):
    return (m["surge"] >= VOL_SURGE_MIN
            and m["recent_turnover_lacs"] >= MIN_TURNOVER_LACS)


# --------------------------------------------------------------------------- #
# screener.in enrichment (market cap + 52-week range, shortlist only)
# --------------------------------------------------------------------------- #
def _to_float(text):
    if text is None:
        return None
    m = re.search(r"-?\d[\d,]*\.?\d*", str(text).replace(",", ""))
    return float(m.group()) if m else None


def fetch_screener(ticker):
    """Return (url, soup) trying consolidated then standalone view."""
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
    """Parse the #top-ratios block into {label: [numbers]}."""
    out = {}
    ul = soup.find(id="top-ratios")
    if not ul:
        return out
    for li in ul.find_all("li"):
        name = li.find(class_="name")
        if not name:
            continue
        nums = [_to_float(n.get_text()) for n in li.find_all(class_="number")]
        out[name.get_text(strip=True)] = [n for n in nums if n is not None]
    return out


def enrich_screener(m):
    """Fetch market cap, name and 52-week high/low from the public screener.in
    page. Returns m with those fields set (None where unavailable)."""
    url, soup = fetch_screener(m["ticker"])
    m["url"] = url
    if soup is None:
        m["mcap"], m["name"], m["w52_high"], m["w52_low"] = None, m["ticker"], None, None
        return m
    top = parse_top_ratios(soup)
    mc = top.get("Market Cap")
    hl = top.get("High / Low")
    name_tag = soup.find("h1")
    m["mcap"] = mc[0] if mc else None
    m["name"] = name_tag.get_text(strip=True) if name_tag else m["ticker"]
    m["w52_high"] = hl[0] if hl and len(hl) >= 1 else None
    m["w52_low"] = hl[1] if hl and len(hl) >= 2 else None
    return m


# --------------------------------------------------------------------------- #
# Price target — plain technical projection off the 52-week range.
# NOT an analyst estimate, NOT a forecast. Two cases:
#   - Price still below its 52-week high: the high itself is the nearest
#     unbroken overhead resistance, so that's the near-term target.
#   - Price already at/above its 52-week high (fresh breakout): project a
#     classic "measured move" — 50% of the prior 52-week range added on top.
# --------------------------------------------------------------------------- #
def price_target(m):
    price = m.get("price")
    hi, lo = m.get("w52_high"), m.get("w52_low")
    if not price or price <= 0 or hi is None or lo is None:
        return None, None, "insufficient data (52W range unavailable)"
    if price < hi:
        target = hi
        basis = "resistance at 52W high"
    else:
        swing = hi - lo
        target = price + 0.5 * swing
        basis = "breakout measured-move (+50% of 52W range)"
    upside = round((target / price - 1) * 100, 1)
    return round(target, 1), upside, basis


# --------------------------------------------------------------------------- #
# Likely surge reason — best-effort recent-headline lookup via Google News RSS.
# No API key needed; free text search scoped to the last NEWS_LOOKBACK_DAYS.
# This is context, not a confirmed cause — volume can move without news too.
# --------------------------------------------------------------------------- #
def surge_reason(name):
    url = ("https://news.google.com/rss/search?q="
           + requests.utils.quote(f"{name} share when:{NEWS_LOOKBACK_DAYS}d")
           + "&hl=en-IN&gl=IN&ceid=IN:en")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return "News lookup unavailable"
        root = ET.fromstring(r.text)
        item = root.find(".//item")
        if item is None:
            return "No recent news found"
        title_el = item.find("title")
        title = title_el.text if title_el is not None else ""
        if " - " in title:
            headline, source = title.rsplit(" - ", 1)
        else:
            headline, source = title, ""
        headline = headline.strip()
        return f"{headline} ({source})" if source else (headline or "No recent news found")
    except (requests.RequestException, ET.ParseError):
        return "News lookup failed"


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def fmt(v, suffix=""):
    return f"{v:g}{suffix}" if isinstance(v, (int, float)) else "n/a"


def _lacs_to_cr(v):
    return v / 100.0 if isinstance(v, (int, float)) else None


def build_report(rows, today, scanned_syms, days):
    lines = []
    lines.append("# Fresh Volume Scan — NSE large-caps (>= Rs 10,000 cr)\n")
    lines.append(
        f"**Date:** {today} · **Universe:** NSE EQ, market cap ≥ Rs {MCAP_MIN:g} cr "
        f"· **Signal:** last {RECENT_DAYS}-day avg volume ≥ {VOL_SURGE_MIN:g}× the "
        f"prior {BASE_DAYS}-day avg · **Source:** NSE bhavcopy + screener.in + "
        "Google News\n")
    lines.append(
        "> Analytical research, **NOT investment advice**. A volume build-up is "
        "direction-agnostic — it can precede a breakdown as easily as a breakout. "
        "The **price target** is a plain technical projection off the 52-week "
        "range (52W high if still below it, else a 50%-of-range measured move on "
        "breakout) — not an analyst estimate or forecast. The **surge reason** is "
        "a best-effort recent-headline match, not confirmed causation — volume "
        "can move without news. Check the chart, filings and delivery quality "
        "before acting.\n")
    lines.append(
        f"Scanned **{scanned_syms}** NSE EQ symbols over **{days}** trading days; "
        f"**{len(rows)}** are ≥ Rs {MCAP_MIN:g} cr with a qualifying volume surge "
        f"(avg turnover ≥ Rs {_lacs_to_cr(MIN_TURNOVER_LACS):g} cr/day).\n")

    if not rows:
        lines.append("_No names cleared the filter this run._")
        return "\n".join(lines)

    lines.append("## Ranked by volume surge\n")
    lines.append("| # | Company | Ticker | Mcap (cr) | Vol surge | 5d avg vol | "
                 "20d avg vol | 5d price | Deliv % | Turnover (cr/day) | "
                 "Price | Target | Upside | Likely reason for surge |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, r in enumerate(rows, 1):
        lines.append(
            f"| {i} | {r['name']} | {r['ticker']} | {fmt(r['mcap'])} | "
            f"**{r['surge']}×** | {int(r['recent_avg_vol']):,} | "
            f"{int(r['base_avg_vol']):,} | {fmt(r['price_chg_5d'], '%')} | "
            f"{fmt(r['deliv'], '%')} | {fmt(_lacs_to_cr(r['recent_turnover_lacs']))} | "
            f"{fmt(r['price'])} | {fmt(r['target'])} ({r['target_basis']}) | "
            f"{fmt(r['upside'], '%')} | {r['reason']} |")

    lines.append("\n## Sources\n")
    for r in rows:
        lines.append(f"- [{r['ticker']}]({r['url']})")
    lines.append(f"\n*Volume surge = mean(last {RECENT_DAYS}d volume) ÷ "
                 f"mean(prior {BASE_DAYS}d volume), NSE EQ series. Liquidity floor: "
                 f"avg turnover ≥ Rs {_lacs_to_cr(MIN_TURNOVER_LACS):g} cr/day. "
                 "Price target: technical projection off the 52-week range, not "
                 "an analyst estimate. Surge reason: best-effort recent-headline "
                 "match via Google News, not confirmed causation. "
                 "Not investment advice.*")
    return "\n".join(lines)


def build_email_html(rows, today):
    head = (
        '<div style="font-family:Arial,sans-serif;color:#1a1a1a;max-width:900px">'
        f'<h2>Fresh Volume Scan — NSE large-caps ({today})</h2>'
        '<p style="background:#fff8e1;border-left:4px solid #f0ad4e;padding:8px 12px;'
        f'font-size:13px">Mcap ≥ Rs {MCAP_MIN:g} cr with last {RECENT_DAYS}-day avg '
        f'volume ≥ {VOL_SURGE_MIN:g}× the prior {BASE_DAYS}-day avg. Price target is '
        'a technical projection, not an analyst estimate. '
        '<b>Not investment advice.</b></p>')
    if not rows:
        return head + "<p>No names cleared the filter this run.</p></div>"
    head += (
        '<table cellpadding="6" cellspacing="0" border="1" '
        'style="border-collapse:collapse;font-size:13px">'
        '<tr style="background:#1f3a5f;color:#fff"><th>#</th><th>Company</th>'
        '<th>Ticker</th><th>Mcap</th><th>Vol surge</th><th>Price</th>'
        '<th>Target</th><th>Upside</th><th>Likely reason</th></tr>')
    body = ""
    for i, r in enumerate(rows, 1):
        reason = r["reason"]
        if len(reason) > 90:
            reason = reason[:87] + "..."
        body += (f"<tr><td>{i}</td><td>{r['name']}</td><td>{r['ticker']}</td>"
                 f"<td>{fmt(r['mcap'])}</td><td><b>{r['surge']}×</b></td>"
                 f"<td>{fmt(r['price'])}</td><td>{fmt(r['target'])}</td>"
                 f"<td>{fmt(r['upside'], '%')}</td><td>{reason}</td></tr>")
    return head + body + "</table></div>"


def send_email(subject, html):
    user = os.environ.get("SMTP_USER")
    pw = os.environ.get("SMTP_PASS")
    to = os.environ.get("EMAIL_TO")
    if not (user and pw and to):
        print("Email not configured (SMTP_USER/SMTP_PASS/EMAIL_TO) — skipping.")
        return
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
    session = nse_session()

    print(f"Collecting {NEEDED} trading days of NSE bhavcopy...")
    history = collect_history(session, NEEDED)
    if len(history) < NEEDED:
        print(f"Only got {len(history)}/{NEEDED} trading days — NSE may be blocking "
              "this IP or markets were shut. Aborting.")
        sys.exit(1)

    series = build_series(history)
    print(f"{len(series)} symbols present across all {len(history)} days.")

    surging = []
    for sym, d in series.items():
        m = compute(sym, d)
        if m and qualifies_volume(m):
            surging.append(m)
    surging.sort(key=lambda m: m["surge"], reverse=True)
    print(f"{len(surging)} symbols pass the volume+liquidity filter. "
          f"Confirming market cap (top {min(len(surging), MAX_LOOKUP)})...")

    rows = []
    for m in surging[:MAX_LOOKUP]:
        enrich_screener(m)
        if m.get("mcap") is not None and m["mcap"] >= MCAP_MIN:
            m["target"], m["upside"], m["target_basis"] = price_target(m)
            m["reason"] = surge_reason(m["name"])
            rows.append(m)
            print(f"  ✓ {m['ticker']}: {m['surge']}× surge, mcap {m['mcap']:g} cr, "
                  f"target {fmt(m['target'])}")
        time.sleep(PAGE_DELAY)

    rows.sort(key=lambda r: r["surge"], reverse=True)
    report = build_report(rows, today, len(series), len(history))

    reports_dir = os.path.join(os.path.dirname(__file__), "reports")
    os.makedirs(reports_dir, exist_ok=True)
    for path in (os.path.join(reports_dir, f"volume-{today}.md"),
                 os.path.join(reports_dir, "volume-latest.md")):
        with open(path, "w", encoding="utf-8") as f:
            f.write(report)
    print(f"Wrote reports/volume-{today}.md ({len(rows)} names)")

    subject = (f"Fresh Volume Scan ({today}): {len(rows)} large-caps with rising "
               "volume")
    if rows:
        subject = (f"Fresh Volume Scan ({today}): {rows[0]['name']} +{rows[0]['surge']}× "
                   f"({len(rows)} names)")
    send_email(subject, build_email_html(rows, today))


if __name__ == "__main__":
    main()
