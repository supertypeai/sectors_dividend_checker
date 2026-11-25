"""Yield maintenance shared by the scrapers and the repair scripts.

yield = dividend / mean(Close of the ex-date's calendar year), yfinance
auto_adjust=False (split-adjusted, dividend-unadjusted — the basis every stored
yield was written on).

Rows recomputed:
  - split-affected: an applied idx_stock_split row with split.date > ex_date
  - yield IS NULL
  - prior-year rows still on a partial-year basis: ex-date year is
    current_year - 1 and updated_on predates Jan 1 of the following year

Rows never touched here: symbols with an idx_ca_bonus event on or after the
ex-date. Yahoo already adjusts Close for bonus shares while the source dividend
stays nominal, so recomputing them would double the yield (MEGA 2025-04-16,
WGSH 2025-07-09); the bonus basis decision is a separate ticket.
"""
import datetime as dt
import logging
from collections import defaultdict, Counter
from zoneinfo import ZoneInfo

PRICE_CHUNK = 75        # tickers per yf.download call
TARGET_TOLERANCE = 0.05
RELATIVE_SKIP = 1e-5    # ignore moves smaller than this relative to the stored value
ABS_SKIP = 3e-6         # ...or this in absolute terms (the column rounds to 7 dp)
_LOCAL_TIMEZONE = "Asia/Bangkok"  # same day boundary as the upcoming scraper's delete

_log = logging.getLogger(__name__)


def fetch_all(client, table, columns):
    """Page a table with a deterministic sort, so offset paging cannot skip rows.

    idx_ca_bonus keys its date as ex_date; everything else uses date.
    """
    fields = {field.strip() for field in columns.split(",")}
    tiebreak = "date" if "date" in fields else "ex_date"
    if tiebreak not in fields:
        raise ValueError(f"fetch_all({table}) needs a 'date' or 'ex_date' column "
                         "for stable pagination")
    rows, start = [], 0
    while True:
        batch = (client.from_(table).select(columns).order("symbol")
                 .order(tiebreak).range(start, start + 999).execute().data)
        rows += batch
        if len(batch) < 1000:
            return rows
        start += 1000


def index_by_symbol(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["symbol"]].append(row)
    return grouped


def split_factor(row, splits_by_symbol, log=_log.info):
    factor = 1.0
    for split in splits_by_symbol.get(row["symbol"], []):
        if split["date"] > row["date"]:
            ratio = float(split["split_ratio"])
            if ratio <= 0:
                log(f"[YIELD] ignoring split with ratio {ratio} "
                    f"({row['symbol']} {split['date']})")
                continue
            factor /= ratio
    return factor


def bonus_deferred(row, bonus_by_symbol):
    """Bonus events that change the per-share basis on or after this ex-date."""
    return any(event["ex_date"] and event["ex_date"] >= row["date"]
               for event in bonus_by_symbol.get(row["symbol"], []))


def select_targets(dividends, splits, bonus, current_year):
    splits_by_symbol, bonus_by_symbol = index_by_symbol(splits), index_by_symbol(bonus)
    targets, skipped = {}, []

    def add(row, reason):
        key = (row["symbol"], row["date"])
        if bonus_deferred(row, bonus_by_symbol):
            if key not in targets:
                skipped.append((row["symbol"], row["date"], reason))
            return
        targets.setdefault(key, {**row, "reason": reason})

    for row in dividends:
        if row["dividend"] is None:
            continue
        if split_factor(row, splits_by_symbol) != 1.0:
            add(row, "split")
        if row["yield"] is None:
            add(row, "null")
        year = int(row["date"][:4])
        if (year == current_year - 1 and row["yield"] is not None
                and (row["updated_on"] or "")[:10] < f"{year + 1}-01-01"):
            add(row, "prior-year")
    return list(targets.values()), skipped


def fetch_mean_closes(pairs, log=_log.info):
    """mean Close per (symbol, year); symbols with no history are returned as no_price."""
    import yfinance as yf  # imported lazily so the selection helpers stay testable

    by_year = defaultdict(list)
    for symbol, year in pairs:
        by_year[year].append(symbol)

    means, no_price = {}, []
    for year in sorted(by_year):
        symbols = sorted(set(by_year[year]))
        for start in range(0, len(symbols), PRICE_CHUNK):
            chunk = symbols[start:start + PRICE_CHUNK]
            frame = yf.download(chunk, start=f"{year}-01-01", end=f"{year}-12-31",
                                progress=False, auto_adjust=False, threads=True)
            for symbol in chunk:
                try:
                    series = frame[("Close", symbol)].dropna()
                except KeyError:
                    no_price.append((symbol, year))
                    continue
                if series.empty:
                    no_price.append((symbol, year))
                    continue
                means[(symbol, year)] = float(series.mean())
            log(f"[YIELD] priced {year} {start + len(chunk)}/{len(symbols)} symbols")
    return means, no_price


def is_change(new_yield, old_yield):
    """True when the recomputed yield differs beyond the column's storage noise."""
    if old_yield is None:
        return True
    return abs(new_yield - old_yield) > max(ABS_SKIP, RELATIVE_SKIP * abs(old_yield))


def build_plan(targets, log=_log.info):
    means, no_price = fetch_mean_closes(
        {(row["symbol"], int(row["date"][:4])) for row in targets}, log=log)
    plan, missing = [], []
    for row in targets:
        mean_close = means.get((row["symbol"], int(row["date"][:4])))
        if not mean_close or mean_close <= 0:
            missing.append(row)
            continue
        new_yield = round(float(row["dividend"]) / mean_close, 10)
        old_yield = row["yield"]
        if old_yield is not None and not is_change(new_yield, old_yield):
            # A prior-year row is only a target because it was computed mid-year.
            # Rewrite it unchanged so updated_on settles it out of the trigger;
            # otherwise every run refetches prices for rows that never move.
            if row["reason"] != "prior-year":
                continue
            new_yield = old_yield
        plan.append({
            "symbol": row["symbol"],
            "date": row["date"],
            "reason": row["reason"],
            "dividend": row["dividend"],
            "mean_close": round(mean_close, 4),
            "old_yield": old_yield,
            "new_yield": round(new_yield, 10),
            "old_updated_on": row["updated_on"],
        })
    return plan, missing, no_price, means


def apply_yields(client, plan, now, log=_log.info):
    """PATCH each row by (symbol, date).

    A partial-column upsert cannot work here: Postgres validates NOT NULL on the
    proposed tuple before ON CONFLICT runs, and idx_dividend has NOT NULL columns
    this payload does not carry.
    """
    written = 0
    for row in plan:
        (client.table("idx_dividend")
         .update({"yield": row["new_yield"], "updated_on": now})
         .eq("symbol", row["symbol"]).eq("date", row["date"]).execute())
        written += 1
        if written % 200 == 0:
            log(f"[YIELD] updated {written}/{len(plan)}")
    return written


def compute_and_upsert_yields(client, current_year=None, log=_log.info):
    """Recompute every stale yield and write it. Returns (written, no_price_rows).

    Split-affected rows stay targets forever (the split join is structural), so
    each run refetches prices for them: ~180 symbol-years, ~3 yf.download chunks,
    roughly two minutes. Writes are suppressed by is_change; drop the split
    trigger only if that fetch ever needs to go away.
    """
    current_year = current_year or dt.date.today().year
    dividends = fetch_all(client, "idx_dividend", "symbol,date,dividend,yield,updated_on")
    splits = [s for s in fetch_all(client, "idx_stock_split", "symbol,date,split_ratio,applied_on")
              if s["applied_on"]]
    bonus = fetch_all(client, "idx_ca_bonus", "symbol,ex_date")

    targets, skipped = select_targets(dividends, splits, bonus, current_year)
    if not targets:
        log("[YIELD] nothing to recompute")
        return 0, []

    plan, missing, _no_price, _means = build_plan(targets, log=log)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    written = apply_yields(client, plan, now, log=log)
    log(f"[YIELD] targets={len(targets)} {dict(Counter(t['reason'] for t in targets))} "
        f"written={written} deferred_bonus={len(skipped)} no_price={len(missing)}")
    return written, missing


def sync_upcoming_dividends(client, retention_days=14, log=_log.info):
    # 14 mirrors _RETENTION_PERIOD in future_dividend_checker.py; that module
    # builds a Supabase client at import, so the value is duplicated on purpose.
    """Copy idx_dividend.dividend into idx_upcoming_dividend.dividend_amount.

    idx_dividend is the source of truth once an ex-date passes; the upcoming row
    freezes as soon as it leaves the scrape window (ITMG/MEDC 2026-11-11 drifted
    until this ran). Update-only: rows past retention are left to the delete.
    """
    cutoff = (dt.datetime.now(ZoneInfo(_LOCAL_TIMEZONE)).date()
              - dt.timedelta(days=retention_days)).isoformat()
    upcoming = (client.from_("idx_upcoming_dividend")
                .select("symbol,ex_date,dividend_amount")
                .gte("payment_date", cutoff).execute().data) or []
    if not upcoming:
        return 0

    sources = {}
    symbols = sorted({row["symbol"] for row in upcoming})
    for start in range(0, len(symbols), 100):
        chunk = symbols[start:start + 100]
        rows = (client.from_("idx_dividend").select("symbol,date,dividend")
                .in_("symbol", chunk).execute().data) or []
        for row in rows:
            sources[(row["symbol"], row["date"])] = row["dividend"]

    updated = 0
    for row in upcoming:
        source = sources.get((row["symbol"], row["ex_date"]))
        if source is None or row["dividend_amount"] is None:
            continue
        if abs(float(source) - float(row["dividend_amount"])) <= 1e-9:
            continue
        (client.table("idx_upcoming_dividend")
         .update({"dividend_amount": source})
         .eq("symbol", row["symbol"]).eq("ex_date", row["ex_date"]).execute())
        updated += 1
        log(f"[SYNC] {row['symbol']} {row['ex_date']} "
            f"{row['dividend_amount']} -> {source}")
    return updated
