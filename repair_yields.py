"""Recompute idx_dividend.yield on one consistent basis (dry run by default).

Rollback: --dump writes the planned rows (old values + new values), --apply-dump
replays that plan without refetching prices, --undo restores the dumped old
values. Selection rules live in yield_helper.

Usage: python repair_yields.py [--dump FILE] [--apply] [--apply-dump FILE] [--undo FILE]
"""
import argparse
import datetime as dt
import json
import os
import sys
from collections import Counter

from dotenv import load_dotenv
from supabase import create_client

from yield_helper import (TARGET_TOLERANCE, apply_yields, build_plan, fetch_all,
                          select_targets)

load_dotenv()


def verify_preview(targets, plan, means):
    """Guard, not validation: confirms every priced split row has a plan entry.

    Both sides use the same formula, so this catches unpriced/skipped rows rather
    than wrong prices; treat a non-zero result as a pipeline bug.
    """
    posted = {(p["symbol"], p["date"]): p["new_yield"] for p in plan}
    fails = []
    for row in targets:
        if row["reason"] != "split":
            continue
        mean_close = means.get((row["symbol"], int(row["date"][:4])))
        if not mean_close:
            continue
        true_yield = float(row["dividend"]) / mean_close
        after = posted.get((row["symbol"], row["date"]), row["yield"])
        if after is None or abs(after / true_yield - 1) > TARGET_TOLERANCE:
            fails.append((row["symbol"], row["date"], after, true_yield))
    return fails


def report(targets, plan, skipped, missing, means):
    filled = [p for p in plan if p["reason"] == "null"]
    prior = [p for p in plan if p["reason"] == "prior-year"]
    split_rows = [p for p in plan if p["reason"] == "split"]
    moves = [p for p in split_rows if p["old_yield"] is not None
             and abs(p["new_yield"] / p["old_yield"] - 1) > TARGET_TOLERANCE]

    print(f"targets: {len(targets)} {dict(Counter(t['reason'] for t in targets))}")
    print(f"deferred (idx_ca_bonus on/after ex-date): {len(skipped)}")
    print(f"no price data (left unchanged): {len(missing)} "
          f"{sorted({(r['symbol'], r['date']) for r in missing})}")
    print(f"writes planned: {len(plan)}  split={len(split_rows)} "
          f"null={len(filled)} prior-year={len(prior)} "
          f"(prior-year rows whose value is within noise are written unchanged "
          f"to settle them out of the trigger)")
    print(f"\nsplit rows moving > {TARGET_TOLERANCE:.0%}: {len(moves)}")
    for row in sorted(moves, key=lambda r: r["new_yield"] / r["old_yield"]):
        print(f"  {row['symbol']:<9} {row['date']} div={row['dividend']!s:<11} "
              f"old={row['old_yield']:<11.6f} new={row['new_yield']:.6f} "
              f"ratio={row['new_yield'] / row['old_yield']:.4f}x")
    print(f"\nNULL fills: {len(filled)} (first 10)")
    for row in sorted(filled, key=lambda r: (r["date"], r["symbol"]))[:10]:
        print(f"  {row['symbol']:<9} {row['date']} div={row['dividend']!s:<11} "
              f"new={row['new_yield']:.6f}")
    prior_moves = sorted((p for p in prior if p["old_yield"]),
                         key=lambda r: -abs(r["new_yield"] / r["old_yield"] - 1))
    print(f"\nprior-year restamps: {len(prior)} | largest 10 moves")
    for row in prior_moves[:10]:
        print(f"  {row['symbol']:<9} {row['date']} old={row['old_yield']:<11.6f} "
              f"new={row['new_yield']:.6f} delta="
              f"{(row['new_yield'] / row['old_yield'] - 1) * 100:+.2f}%")

    fails = verify_preview(targets, plan, means)
    print(f"\nplan guard — split rows with no usable plan entry or outside "
          f"±{TARGET_TOLERANCE:.0%}: {len(fails)}")
    for row in fails[:10]:
        print(f"  FAIL {row[0]} {row[1]} after={row[2]} true={row[3]:.6f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write instead of dry run")
    parser.add_argument("--dump", help="write the plan to a JSON rollback file")
    parser.add_argument("--apply-dump", help="write the plan stored in a dump file")
    parser.add_argument("--undo", help="restore the values listed in a dump file")
    args = parser.parse_args()

    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    if not url or not key:
        parser.error("SUPABASE_URL and SUPABASE_KEY are required")
    client = create_client(url, key)

    if args.undo:
        with open(args.undo) as handle:
            previous = json.load(handle)
        for row in previous:
            client.table("idx_dividend").update({
                "yield": row["old_yield"],
                "updated_on": row["old_updated_on"],
            }).eq("symbol", row["symbol"]).eq("date", row["date"]).execute()
        print(f"restored {len(previous)} rows from {args.undo}")
        return

    if args.apply_dump:
        with open(args.apply_dump) as handle:
            previous = json.load(handle)
        required = {"symbol", "date", "new_yield"}
        broken = [row for row in previous if not required.issubset(row)]
        if broken:
            parser.error(f"{args.apply_dump}: {len(broken)} row(s) missing "
                         f"{sorted(required)} (first: {broken[0]})")
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        written = apply_yields(client, previous, now, log=print)
        print(f"updated {written} yields from {args.apply_dump} (updated_on = {now})")
        return

    dividends = fetch_all(client, "idx_dividend", "symbol,date,dividend,yield,updated_on")
    splits = [s for s in fetch_all(client, "idx_stock_split", "symbol,date,split_ratio,applied_on")
              if s["applied_on"]]
    bonus = fetch_all(client, "idx_ca_bonus", "symbol,ex_date")
    targets, skipped = select_targets(dividends, splits, bonus, dt.date.today().year)
    plan, missing, _no_price, means = build_plan(targets, log=print)
    report(targets, plan, skipped, missing, means)

    if args.dump:
        with open(args.dump, "w") as handle:
            json.dump(plan, handle, indent=2)
        print(f"\nrollback snapshot written to {args.dump}")

    if not args.apply:
        print("\ndry run: nothing written (pass --apply to update idx_dividend)")
        return

    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    written = apply_yields(client, plan, now, log=print)
    print(f"updated {written} yields in idx_dividend (updated_on = {now})")


if __name__ == "__main__":
    sys.exit(main())
