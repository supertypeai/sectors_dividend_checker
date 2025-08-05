from bs4        import BeautifulSoup
from dotenv     import load_dotenv
from supabase   import create_client, Client
from datetime   import datetime, date

import logging
import pandas as pd
import requests
import time
import yfinance as yf
import numpy as np
import os


load_dotenv()


# Setup Logging
logging.basicConfig(
    filename='scrapper.log',
    level=logging.INFO, # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")


def check_start_year():
    """ 
    Check if the current date is in the first week of the first month of the year.
    """
    today_date = date.today()
    # Check if it is the first week of the first month in the year
    return today_date.month == 1 and today_date.day <= 7


class DividendChecker:
    def __init__(self, supabase_client: Client, last_n_day: int = 7):
        """ 
        DividendChecker class to scrape dividend data from SahamIDX and manage it in a database.

        Args:
            supabase_client (Client): Supabase client instance for database operations.
            last_n_day (int): Number of days to look back for dividend data. Default is 7
        """
        self.url = "https://sahamidx.com/?view=Stock.Cash.Dividend&path=Stock&field_sort=rg_ng_ex_date&sort_by=DESC&page={page}"
        self.supabase_client = supabase_client
        self.start_date = (pd.Timestamp.now("Asia/Bangkok") - pd.Timedelta(days=last_n_day - 1)).strftime("%Y-%m-%d")
        self.end_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        self.retrieved_records: list[dict] = []
        self.allowed_symbols = [k['symbol'][:4] for k in
                                self.supabase_client.from_("idx_company_profile").select("symbol").execute().data]

    def get_dividend_records(self, include_payment_date: bool = False):
        """ 
        Scrapes dividend data from SahamIDX and stores it in the retrieved_records list.

        Args:
            include_payment_date (bool): If True, includes payment date in the retrieved records. Default is False.
        """
        attempt = 1
        max_attempt = 10
        
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
                    table = soup.find("table", {"class": "tbl_border_gray"})
                
                    if not table:
                        LOGGER.info("No table found on page. Stopping.")
                        keep_scraping = False
                        continue

                    rows = table.find_all("tr", recursive=False)[1:]
                    
                    if not rows:
                        LOGGER.info("No more data rows found. Stopping scrape.")
                        keep_scraping = False 
                        continue
                    
                    for row in rows:
                        try:
                            # Skip if the row does not have enough columns
                            cols = row.find_all("td")
                            if len(cols) < 8:
                                continue
                            
                            # Get reguler & negosiasi ex-date
                            date = datetime.strptime(cols[6].text.strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
                            if date < self.start_date:
                                LOGGER.info(f"Stop condition met: Found Ex-Date {date} which is older than start date.")
                                keep_scraping = False
                                break
                            
                            # Get symbol
                            symbol = cols[1].find("a").text.strip()
                            if not symbol or symbol not in self.allowed_symbols:
                                continue

                            # Get dividend original
                            dividend_original = float(cols[3].text.strip())

                            # Get payment date
                            payment_date = datetime.strptime(cols[10].text.strip(), "%d-%b-%Y").strftime("%Y-%m-%d")
    
                            # Adjust the symbol
                            adjusted_symbol = symbol + ".JK"
                            
                            # Validation for data in a range start_date and end_date
                            if not (self.start_date <= date <= self.end_date):
                                continue    
                            
                            # Data valid to be upserted
                            data_dict = {
                                    "symbol": adjusted_symbol,
                                    "date": date,
                                    "dividend_original": dividend_original,
                                    "dividend": dividend_original,
                                    "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S"),
                                }

                            if include_payment_date:
                                data_dict["payment_date"] = payment_date

                            LOGGER.info(f'[FETCHING] {data_dict}')

                            self.retrieved_records.append(data_dict)

                        except (ValueError, IndexError):
                            continue
                    
                    page += 1 

                break
            except Exception as e:
                if (attempt == max_attempt):
                    LOGGER.error(f"\t[ATTEMPTS FAILED] Failed after {max_attempt} attempts. | {e}")
                else:
                    LOGGER.error(f"\t[FAILED] Failed after {attempt} attempt(s). Retrying after 2 seconds... | {e}")
                    time.sleep(2)
                attempt += 1
    
    def check_fill_missing_dividend(self, 
                                    is_saved: bool= True,
                                    cutoff_date: str = "2020-01-01", 
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
            table = soup.find("table", {"class": "tbl_border_gray"})
            rows = table.find_all("tr", recursive=False)[1:] if table else []
            if not rows:
                LOGGER.info("Reached the last page with data. Process complete.")
                keep_scraping = False
                continue
            
            for row in rows:
                try:
                    cols = row.find_all("td")
                    if len(cols) < 10:
                        continue
                    
                    # Get date
                    date_from_site_dt = datetime.strptime(cols[6].text.strip(), "%d-%b-%Y")

                    # Stop check using the cutoff date
                    if date_from_site_dt < cutoff_dt:
                        LOGGER.info(f"Stop condition met: Found date {date_from_site_dt.strftime('%Y-%m-%d')} which is older than cutoff {cutoff_date}.")
                        keep_scraping = False
                        break 
                    
                    # Get symbol
                    symbol = cols[1].find("a").text.strip()
                    if symbol not in self.allowed_symbols:
                        continue

                    # Adjust the symbol
                    adjusted_symbol = f"{symbol}.JK"

                    # Format the date to "YYYY-MM-DD"
                    date_str = date_from_site_dt.strftime("%Y-%m-%d")

                    # Get dividend
                    dividend_original = float(cols[3].text.strip())

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
            
                except (ValueError, IndexError) as error:
                    continue
            
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

    def upsert_to_db(self):
        """
        Upserts the retrieved dividend records to the database.
        """
        if not self.retrieved_records:
            LOGGER.warning("No records to upsert to database. All data is up to date")
            raise SystemExit(0)

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
    stock_split_checker.get_dividend_records()

    # Run the dividend check and fill missing data
    # stock_split_checker.check_fill_missing_dividend()

    # Upsert DB
    stock_split_checker.upsert_to_db()

    logging.info(f"update {date.today()} dividend data")

    if check_start_year():
        stock_split_checker.upsert_yield_in_db()
  