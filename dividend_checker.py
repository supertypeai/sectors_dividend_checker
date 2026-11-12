from bs4        import BeautifulSoup
from dotenv     import load_dotenv
from supabase   import create_client, Client
from datetime   import datetime, date

import logging
import math
import pandas as pd
import requests
import re
import time
import yfinance as yf
import numpy as np
import os


load_dotenv()


# Setup Logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler('scrapper.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)

LOGGER.addHandler(file_handler)

LOGGER.info("Init Global Variable")


CONVERSION_RATE_URL = (
    "https://raw.githubusercontent.com/supertypeai/"
    "sectors_get_conversion_rate/master/conversion_rate.json"
)
CURRENCY_CODE = {
    "US$": "USD", "USD": "USD",
    "S$": "SGD", "SGD": "SGD",
    "RP": "IDR", "IDR": "IDR",
    "RM": "MYR", "MYR": "MYR",
    "HK$": "HKD", "HKD": "HKD",
    "NT$": "TWD", "TWD": "TWD",
    "A$": "AUD", "AUD": "AUD",
    "C$": "CAD", "CAD": "CAD",
    "RMB": "CNY", "CNY": "CNY", "CN¥": "CNY",
    "EUR": "EUR", "€": "EUR",
    "GBP": "GBP", "£": "GBP",
    "PHP": "PHP", "₱": "PHP",
    "THB": "THB", "฿": "THB",
    "INR": "INR", "₹": "INR",
    "KRW": "KRW", "₩": "KRW",
    "JPY": "JPY",
    "CHF": "CHF",
}
UNKNOWN_CURRENCY_MARKERS = ("$", "¥")
_CURRENCY_MARKERS = sorted(
    set(CURRENCY_CODE) | set(UNKNOWN_CURRENCY_MARKERS),
    key=len,
    reverse=True,
)
CURRENCY_AMOUNT_RE = re.compile(
    r"^\s*(?P<currency>"
    + "|".join(re.escape(marker) for marker in _CURRENCY_MARKERS)
    + r")\s*(?P<amount>[\d,]+(?:\.\d+)?)\s*$",
    re.IGNORECASE,
)

_conversion_rates = None
USD_COMPARISON_RATIO = 1e-4


def get_conversion_rates() -> dict:
    """Fetch the conversion-rate feed once per run."""
    global _conversion_rates
    if _conversion_rates is None:
        response = requests.get(CONVERSION_RATE_URL, timeout=10)
        response.raise_for_status()
        _conversion_rates = response.json()
        LOGGER.info(
            f"FX feed loaded ({_conversion_rates.get('datetime')})"
        )
    return _conversion_rates


def convert_to_idr(amount: float, currency: str) -> float:
    """Convert an explicitly marked currency amount to IDR."""
    if currency == "IDR":
        return amount

    try:
        rates = get_conversion_rates()
    except (KeyError, TypeError, ValueError, requests.RequestException) as error:
        raise ValueError(f"FX feed unavailable: {error}") from error

    try:
        usd_idr = float(rates["USD"]["IDR"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Invalid USD/IDR rate") from error

    if not math.isfinite(usd_idr) or usd_idr <= 0:
        raise ValueError("Invalid USD/IDR rate")

    if currency == "USD":
        return amount * usd_idr

    usd_rates = rates.get("USD", {})
    if currency in usd_rates:
        try:
            usd_per_unit = 1.0 / float(usd_rates[currency])
        except (KeyError, TypeError, ValueError, ZeroDivisionError) as error:
            raise ValueError(f"No conversion rate for {currency}") from error

        if not math.isfinite(usd_per_unit) or usd_per_unit <= 0:
            raise ValueError(f"Invalid {currency}/USD rate")
        return amount * usd_per_unit * usd_idr

    try:
        sgd_per_unit = float(rates[currency]["SGD"])
        usd_sgd = float(rates["USD"]["SGD"])
        if (
            not math.isfinite(sgd_per_unit)
            or not math.isfinite(usd_sgd)
            or sgd_per_unit <= 0
            or usd_sgd <= 0
        ):
            raise ValueError(f"Invalid {currency}/SGD rate")
    except (KeyError, TypeError) as error:
        raise ValueError(f"No conversion rate for {currency}") from error

    return amount * sgd_per_unit / usd_sgd * usd_idr


def parse_dividend_amount(raw_amount: str) -> float:
    """Return dividend in IDR. Only explicit non-IDR markers are converted."""
    match = CURRENCY_AMOUNT_RE.match(raw_amount)
    if not match:
        return float(raw_amount)

    marker = re.sub(r"\s+", "", match.group("currency")).upper()
    currency = CURRENCY_CODE.get(marker)
    if currency is None:
        raise ValueError(f"Unsupported currency marker: {marker}")

    amount = float(match.group("amount").replace(",", ""))
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError(f"Invalid dividend amount: {marker} {amount}")

    idr_amount = convert_to_idr(amount, currency)
    if currency != "IDR":
        LOGGER.info(f"[FX] {marker} {amount} -> IDR {idr_amount} ({currency})")
    return idr_amount


def skip_tag(row: dict) -> str:
    """Terse reason code for a failed record, for monitoring greps."""
    msg = str(row.get("reason") or row.get("error") or "").lower()
    if "converted to idr" in msg:
        return "usd_converted"
    if "conversion failed" in msg:
        return "usd_convert_failed"
    if (
        "fx feed unavailable" in msg
        or "no conversion rate" in msg
        or "/usd rate" in msg
        or "/sgd rate" in msg
        or "/idr rate" in msg
    ):
        return "fx_no_rate"
    if "unsupported currency marker" in msg:
        return "bad_currency"
    if "invalid dividend amount" in msg:
        return "invalid_amount"
    if "invalid previous dividend" in msg:
        return "invalid_previous"
    if "could not convert string to float" in msg:
        return "not_a_number"
    if "does not match format" in msg or "unconverted data" in msg:
        return "bad_date"
    if "missing payment_date" in msg:
        return "missing_payment_date"
    return "other"


def check_start_year():
    """ 
    Check if the current date is in the first week of the first month of the year.
    """
    today_date = date.today()
    # Check if it is the first week of the first month in the year
    return today_date.month == 1 and today_date.day <= 7


class DividendChecker:
    def __init__(self, supabase_client: Client, last_n_day: int = 30):
        """ 
        DividendChecker class to scrape dividend data from SahamIDX and manage it in a database.

        Args:
            supabase_client (Client): Supabase client instance for database operations.
            last_n_day (int): Number of days to look back for dividend data. Default is 7
        """
        self.url = "https://www.new.sahamidx.com/?/deviden/page/{page}"
        self.supabase_client = supabase_client
        self.start_date = (pd.Timestamp.now("Asia/Bangkok") - pd.Timedelta(days=last_n_day - 1)).strftime("%Y-%m-%d")
        self.end_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        self.retrieved_records: list[dict] = []
        self.failed_records: list[dict] = []
        self._previous_dividends = {}
        self.allowed_symbols = [k['symbol'][:4] for k in
                                self.supabase_client.from_("idx_company_profile").select("symbol").execute().data]

    def get_previous_dividend(self, symbol: str, ex_date: str):
        """Latest idx_dividend value strictly before ex_date, cached per symbol."""
        rows = self._previous_dividends.get(symbol)
        if rows is None:
            response = (
                self.supabase_client.from_("idx_dividend")
                .select("date,dividend")
                .eq("symbol", symbol)
                .order("date", desc=True)
                .execute()
            )
            rows = response.data or []
            rows.sort(key=lambda row: str(row.get("date")), reverse=True)
            self._previous_dividends[symbol] = rows

        for row in rows:
            date_value = row.get("date")
            dividend = row.get("dividend")
            if date_value is None or dividend is None:
                continue
            if hasattr(date_value, "isoformat"):
                date_value = date_value.isoformat()
            if str(date_value) < ex_date:
                return dividend
        return None

    def resolve_amount(self, raw_amount: str, symbol: str, ex_date: str):
        """Resolve an Amount cell to (value, comparison_failure)."""
        if CURRENCY_AMOUNT_RE.match(raw_amount):
            return parse_dividend_amount(raw_amount), None

        amount = float(raw_amount)
        if not math.isfinite(amount) or amount <= 0:
            return amount, {
                "reason": "invalid dividend amount",
                "previous_dividend": None,
                "ratio": None,
            }
        previous_dividend = self.get_previous_dividend(symbol, ex_date)
        if previous_dividend is None:
            LOGGER.info(
                f"[AMOUNT] {symbol} {ex_date} raw={amount} "
                "no previous dividend, treated as IDR"
            )
            return amount, None

        try:
            previous_dividend = float(previous_dividend)
        except (TypeError, ValueError):
            return amount, {
                "reason": "invalid previous dividend",
                "previous_dividend": previous_dividend,
                "ratio": None,
            }

        if not math.isfinite(previous_dividend) or previous_dividend <= 0:
            return amount, {
                "reason": "invalid previous dividend",
                "previous_dividend": previous_dividend,
                "ratio": None,
            }

        ratio = amount / previous_dividend
        if ratio <= USD_COMPARISON_RATIO:
            try:
                converted_idr = convert_to_idr(amount, "USD")
            except ValueError as error:
                LOGGER.error(
                    f"[AMOUNT] {symbol} {ex_date} raw={amount} "
                    f"detected USD but conversion failed: {error}"
                )
                return amount, {
                    "reason": (
                        "detected USD from previous dividend but "
                        f"conversion failed: {error}"
                    ),
                    "previous_dividend": previous_dividend,
                    "ratio": ratio,
                }

            LOGGER.info(
                f"[AMOUNT] {symbol} {ex_date} raw={amount} "
                f"previous={previous_dividend} ratio={ratio:.3e} "
                f"-> detected USD, converted to IDR {converted_idr}"
            )
            return converted_idr, {
                "reason": "detected USD from previous dividend; converted to IDR",
                "previous_dividend": previous_dividend,
                "ratio": ratio,
                "converted_idr": converted_idr,
            }

        LOGGER.info(
            f"[AMOUNT] {symbol} {ex_date} raw={amount} "
            f"previous={previous_dividend} ratio={ratio:.3e} -> IDR"
        )
        return amount, None

    def get_dividend_records(self, include_payment_date: bool = False):
        """ 
        Scrapes dividend data from SahamIDX and stores it in the retrieved_records list.

        Args:
            include_payment_date (bool): If True, includes payment date in the retrieved records. Default is False.
        """
        attempt = 1
        max_attempt = 10
        
        LOGGER.info(f'Scrape for start date: {self.start_date} and end date: {self.end_date}')

        while (attempt <= max_attempt):
            try:
                page = 1                      
                keep_scraping = True

                while keep_scraping:
                    current_url = self.url.format(page=page)
                    LOGGER.info(f"Fetching page {page}...")
                    
                    response = requests.get(current_url)
                    if response.status_code != 200:
                        raise Exception("Error retrieving data from SahamIDX")

                    soup = BeautifulSoup(response.text, "lxml")
                    rows = soup.find_all("tr")
                    data_found_on_page = False

                    for row in rows:
                        row_symbol = None
                        row_ex_date = None
                        row_amount = None
                        try:
                            # Get necessary data cell
                            symbol = row.find("td", {"data-header": "Nama"})
                            dividend = row.find("td", {"data-header": "Amount"})
                            ex_date = row.find("td", {"data-header": "Ex Date"})
                            payment_date = row.find("td", {"data-header": "Payment Date"})
                            recording_date = row.find("td", {"data-header": "Recording Date"})
                            cum_date = row.find("td", {"data-header": "Cum Date"})
                    
                            if not (symbol and dividend and ex_date):
                                continue
                            
                            data_found_on_page = True

                            # Prepare symbol 
                            row_symbol = symbol.text.strip()
                            if row_symbol not in self.allowed_symbols:
                                continue 
                            
                            symbol = row_symbol + '.JK'
                            row_symbol = symbol

                            # Prepare Ex Date 
                            row_ex_date = ex_date.text.strip()
                            ex_date = datetime.strptime(row_ex_date, "%d-%b-%Y").strftime("%Y-%m-%d")
                            row_ex_date = ex_date
                            
                            if ex_date < self.start_date:
                                LOGGER.info(f"Stop condition met: Found Ex-Date {ex_date} which is older than start date {self.start_date}.")
                                keep_scraping = False
                                break

                            # Validation for data in a range start_date and end_date
                            if not (self.start_date <= ex_date <= self.end_date):
                                continue  

                            # Prepare divident original
                            row_amount = dividend.text.strip()
                            dividend, comparison_failure = self.resolve_amount(
                                row_amount, symbol, ex_date
                            )

                            # Prepare Cum Date
                            cum_date_str = cum_date.text.strip()
                            cum_date = datetime.strptime(cum_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

                            # Prepare Recording Date
                            recording_date_str = recording_date.text.strip()
                            recording_date = datetime.strptime(recording_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

                            # Data valid to be upserted
                            data_dict = {
                                "symbol": symbol,
                                "date": ex_date,
                                "dividend_original": dividend,
                                "dividend": dividend,
                                "recording_date": recording_date,
                                "cum_date": cum_date,
                                "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S"),
                            }

                            if include_payment_date:
                                if not payment_date:
                                    self.failed_records.append({
                                        "symbol": symbol,
                                        "ex_date": ex_date,
                                        "amount": row_amount,
                                        "error": "missing payment_date",
                                    })
                                    continue

                                payment_date_str = payment_date.text.strip()
                                payment_date = datetime.strptime(payment_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
                            
                                data_dict["payment_date"] = payment_date

                            if comparison_failure:
                                self.failed_records.append({
                                    "symbol": symbol,
                                    "ex_date": ex_date,
                                    "amount": row_amount,
                                    **comparison_failure,
                                })

                            LOGGER.info(f'[FETCHING] {data_dict}')
                            self.retrieved_records.append(data_dict)

                        except (ValueError, TypeError, AttributeError) as error:
                            self.failed_records.append({
                                "symbol": row_symbol,
                                "ex_date": row_ex_date,
                                "amount": row_amount,
                                "error": str(error),
                            })
                            continue

                    if not data_found_on_page:
                        LOGGER.info("No more data rows found on this page. Stopping scrape")
                        keep_scraping = False

                    time.sleep(1)
                    page += 1 

                break

            except Exception as error:
                if (attempt == max_attempt):
                    LOGGER.error(f"\t[ATTEMPTS FAILED] Failed after {max_attempt} attempts. | {error}")
                else:
                    LOGGER.error(f"\t[FAILED] Failed after {attempt} attempt(s). Retrying after 2 seconds... | {error}")
                    time.sleep(2)
                attempt += 1

    def check_fill_missing_dividend(self, 
                                    is_saved: bool= True,
                                    cutoff_date: str = "2025-10-08", 
                                    db_table_name: str = "idx_dividend"):
        """
        Scrapes all dividends from SahamIDX and inserts any that are missing from the database.
        Stops when it encounters a dividend date older than the cutoff_date.

        Args:
            is_saved (bool): If True, saves newly inserted records to a CSV file. Default is True.
            cutoff_date (str): The date in "YYYY-MM-DD" format to stop scraping when
                a dividend date older than this is found. Default is "2020-01-01".
            db_table_name (str): The name of the database table to check for existing records.

        """
        page = 1
        keep_scraping = True
        newly_inserted_records = []
        
        # Convert the string cutoff_date to a datetime object for comparison
        cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")
        
        while keep_scraping:
            current_url = self.url.format(page=page)
            LOGGER.info(f"Processing page {page}...")

            try:
                response = requests.get(current_url, timeout=15)
                if response.status_code != 200:
                    LOGGER.info(f"Error fetching page {page}, status code {response.status_code}. Stopping.")
                    break
            except requests.exceptions.RequestException as e:
                LOGGER.error(f"Network error on page {page}: {e}. Stopping.")
                break

            soup = BeautifulSoup(response.text, "lxml")
            
            rows = soup.find_all("tr")
            data_found_on_page = False

            for row in rows:
                row_symbol = None
                row_date = None
                row_amount = None
                try:
                    symbol_cell = row.find("td", {"data-header": "Nama"})
                    dividend_cell = row.find("td", {"data-header": "Amount"})
                    ex_date_cell = row.find("td", {"data-header": "Ex Date"})

                    if not (symbol_cell and dividend_cell and ex_date_cell):
                        continue
                        
                    data_found_on_page = True

                    # Get date
                    ex_date_str = ex_date_cell.text.strip()
                    date_from_site_dt = datetime.strptime(ex_date_str, "%d-%b-%Y")

                    # Stop check using the cutoff date 
                    if date_from_site_dt < cutoff_dt:
                        LOGGER.info(f"Stop condition met: Found date {date_from_site_dt.strftime('%Y-%m-%d')} which is older than cutoff {cutoff_date}.")
                        keep_scraping = False
                        break
                    
                    # Get symbol (Using your confirmed simple logic)
                    symbol = symbol_cell.text.strip()
                    if symbol not in self.allowed_symbols:
                        continue

                    # Adjust the symbol
                    adjusted_symbol = f"{symbol}.JK"

                    # Format the date to "YYYY-MM-DD"
                    date_str = date_from_site_dt.strftime("%Y-%m-%d")
                    row_symbol = adjusted_symbol
                    row_date = date_str

                    # Get dividend
                    dividend_str = dividend_cell.text.strip()
                    row_amount = dividend_str
                    dividend_original, comparison_failure = self.resolve_amount(
                        dividend_str, adjusted_symbol, date_str
                    )
                    if comparison_failure:
                        self.failed_records.append({
                            "symbol": adjusted_symbol,
                            "ex_date": date_str,
                            "amount": dividend_str,
                            **comparison_failure,
                        })

                    # Check Supabase data if a record with this key (symbol, date) already exists
                    count_res = self.supabase_client.from_(db_table_name) \
                                    .select('symbol', count='exact') \
                                    .eq("symbol", adjusted_symbol) \
                                    .eq("date", date_str) \
                                    .execute()
                    
                    # Check if the count is zero, meaning no existing record
                    if count_res.count == 0: 
                        # New record to insert
                        if date_str <= self.end_date:
                            LOGGER.info(f"New record found: {symbol} on {date_str}. Inserting")

                            data_dict = {
                                "symbol": adjusted_symbol,
                                "date": date_str,
                                "dividend_original": dividend_original,
                                "dividend": dividend_original, 
                                "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S"),
                            }

                            # Insert into the database
                            try:
                                self.supabase_client.from_(db_table_name).insert(data_dict).execute()
                            except Exception as error:
                                LOGGER.error(f"Error inserting new data {error}")

                            newly_inserted_records.append(data_dict)
                    else:
                        pass 
            
                except (ValueError, TypeError, IndexError, AttributeError) as error:
                    self.failed_records.append({
                        "symbol": row_symbol,
                        "ex_date": row_date,
                        "amount": row_amount,
                        "error": str(error),
                    })
                    continue
            
            if not data_found_on_page:
                LOGGER.info("Reached the last page with data. Process complete.")
                keep_scraping = False

            if not keep_scraping:
                break
                
            page += 1
            time.sleep(2) 
        
        # Saved to csv
        if is_saved and newly_inserted_records:
            LOGGER.info('Saved as csv to check')
            df = pd.DataFrame(newly_inserted_records)
            df.to_csv("new_data_to_insert_continue.csv", index=False)

        LOGGER.info(f"\nBackfill complete. Inserted {len(newly_inserted_records)} new records.")
        self.raise_for_failed_records()

    def raise_for_failed_records(self):
        """
        Logs scraped rows that could not be parsed after DB updates.
        """
        if not self.failed_records:
            return

        def format_failure(row):
            parts = [
                f"{row.get('symbol') or 'UNKNOWN'}",
                f"ex_date={row.get('ex_date') or 'UNKNOWN'}",
                f"amount={row.get('amount') or 'UNKNOWN'}",
            ]
            if row.get("previous_dividend") is not None:
                parts.append(f"previous={row['previous_dividend']}")
            if row.get("ratio") is not None:
                parts.append(f"ratio={row['ratio']}")
            if row.get("converted_idr") is not None:
                parts.append(f"converted={row['converted_idr']}")
            parts.append(f"({row.get('reason') or row.get('error')})")
            return " ".join(parts)

        for row in self.failed_records:
            LOGGER.warning(f"[SKIPPED] {format_failure(row)} [{skip_tag(row)}]")

        summary = " ".join(
            f"{row.get('symbol') or 'UNKNOWN'}:{skip_tag(row)}"
            for row in self.failed_records
        )
        LOGGER.warning(f"[SKIPPED-TOTAL] {len(self.failed_records)} | {summary}")

    def upsert_to_db(self):
        """
        Upserts the retrieved dividend records to the database.
        """
        if not self.retrieved_records:
            LOGGER.warning("No records to upsert to database. All data is up to date")
            return

        try:
            self.supabase_client.table("idx_dividend").upsert(
                self.retrieved_records
            ).execute()
            LOGGER.info(
                f"Successfully upserted {len(self.retrieved_records)} data to database"
            )
        except Exception as error:
            raise Exception(f"Error upserting to database: {error}")

    def upsert_yield_in_db(self):
        """
        Calculates the yield for each dividend record in the database where the yield is missing,
        and updates the database with the calculated yield values.
        """
        database_data = supabase_client.from_("idx_dividend").select("*").execute().data
        db_df = pd.DataFrame(database_data)

        count = 0
        for index, row in db_df.iterrows():
            if pd.isna(row['yield']):
                ticker = row['symbol']
                dividend_date = row['date']
                dividend_year = int(dividend_date.split("-")[0])
                current_year = datetime.now().year

                if (dividend_year < current_year):
                    # If starting from that year
                    start_date = f"{dividend_year}-01-01"
                    end_date = f"{dividend_year}-12-31"
                    count += 1

                    stock = yf.Ticker(ticker).history(start=start_date, end=end_date, auto_adjust=False)
                    stock = stock[["Close"]]  # Get only the Close data
                    mean_val = stock.mean().values[0]
                    yield_val = row['dividend'] / mean_val
                    db_df.at[index, 'yield'] = yield_val
                    db_df.at[index, 'updated_on'] = pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S")
                    LOGGER.info(f"[UPDATING YIELD] {ticker} {dividend_date} yield : {yield_val}")

        db_df = db_df.replace({np.nan: None})

        # Upsert to db the result
        try:
            self.supabase_client.table("idx_dividend").upsert(
                db_df.to_dict(orient="records")
            ).execute()
            LOGGER.info(
                f"Successfully updated {count} yield data in database"
            )
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")


if __name__ == "__main__":
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    stock_split_checker = DividendChecker(supabase_client)
    stock_split_checker.get_dividend_records(include_payment_date=True)

    # Run the dividend check and fill missing data
    # stock_split_checker.check_fill_missing_dividend()

    # Upsert DB
    stock_split_checker.upsert_to_db()

    logging.info(f"update {date.today()} dividend data")

    if check_start_year():
        stock_split_checker.upsert_yield_in_db()

    stock_split_checker.raise_for_failed_records()
  
