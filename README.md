# Dividend Checker

IDX dividend scrapers run by GitHub Actions.

- `dividend_checker.py` - daily scrape into `idx_dividend`.
- `future_dividend_checker.py` - weekday scrape into `idx_upcoming_dividend`, then
  delete rows 14 days after payment date.

Data source: [sahamidx.com](https://sahamidx.com/?view=Stock.Cash.Dividend&path=Stock&field_sort=rg_ng_ex_date&sort_by=DESC&page=1)

## FX

- Explicit non-IDR amounts (`US$`, `S$`, `RM`, `€`, etc.) are converted to IDR using
  [conversion_rate.json](https://github.com/supertypeai/sectors_get_conversion_rate/blob/master/conversion_rate.json).
- Bare amounts are treated as IDR unless they are `<= 1e-4` of the symbol's previous
  `idx_dividend` value. Those are converted to IDR and flagged for review.
- `dividend` and `dividend_original` are stored in IDR.

## Failures

Rows that cannot be parsed, converted, or validated are collected. Valid rows are upserted
first, then the run raises at the end so GitHub Actions fails. Examples: `xstrix`, unknown
currency markers, missing rates, invalid previous dividends.

Upserts insert new rows and update existing rows on their primary key. Historical scrape
samples are in `records/`.
