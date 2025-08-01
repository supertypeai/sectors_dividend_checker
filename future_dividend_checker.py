import json
import os
import pandas as pd
import logging
from imp import reload

from dotenv import load_dotenv
import requests
from supabase import create_client, Client
from typing import final

from dividend_checker import DividendChecker

LOG_FILENAME = 'scrapper.log'

def initiate_logging(LOG_FILENAME):
    reload(logging)

    formatLOG = '%(asctime)s - %(levelname)s: %(message)s'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO, format=formatLOG)
    logging.info('Program started')

# Default next N days of dividend ex-date to fetch
_DEFAULT_TIMEFRAME = 14  # days
# Default retention period after dividend payment date in database
_RETENTION_PERIOD = 14  # days
_LOCAL_TIMEZONE = 'Asia/Bangkok'
_LOCAL_TODAY = pd.Timestamp.now(_LOCAL_TIMEZONE).date()

load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_KEY")
_supabase_client = create_client(_SUPABASE_URL, _SUPABASE_KEY)


class FutureDividendChecker(DividendChecker):
    def __init__(self, supabase_client: Client, future_n_day=14):
        super().__init__(supabase_client)
        # override the start_date and end_date to future date
        self.start_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        self.end_date = (pd.Timestamp.now("Asia/Bangkok") + pd.Timedelta(days=future_n_day)).strftime("%Y-%m-%d")

    @final
    def get_dividend_records(self, include_payment_date=True):
        super().get_dividend_records(include_payment_date=True)
        # Transform the record names accordingly to match the future_dividend table attributes
        for record in self.retrieved_records:
            record["ex_date"] = record.pop("date")
            record["dividend_amount"] = record.pop("dividend")
            record.pop("dividend_original")

    @final
    def upsert_to_db(self):
        if not self.retrieved_records:
            print("No records to upsert to database. All data is up to date")
            raise SystemExit(0)

        try:
            self.supabase_client.table("idx_upcoming_dividend").upsert(
                self.retrieved_records
            ).execute()
            print(
                f"Successfully upserted {len(self.retrieved_records)} data to database"
            )
            # Insert news, commented for now
            # print("Sending data to external endpoint")
            # api_key = os.getenv("API_KEY")
            # headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            # response = requests.post(
            #     "https://sectors-news-endpoint.fly.dev/dividend",
            #     headers=headers,
            #     data=json.dumps(self.retrieved_records)
            # )
            # if response.status_code == 200:
            #     print("Successfully sent data to external endpoint")
            # else:
            #     print(f"Failed to send data to external endpoint. Status code: {response.status_code}, {response.text}")
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")

    @final
    def upsert_yield_in_db(self):
        raise NotImplementedError("Future dividend does not require yield update")


if __name__ == "__main__":

    initiate_logging(LOG_FILENAME)

    # Update upcoming dividend data
    future_dividend_checker = FutureDividendChecker(_supabase_client)
    future_dividend_checker.get_dividend_records()
    future_dividend_checker.upsert_to_db()

    # Delete past dividend data from the same table
    deletion_date = _LOCAL_TODAY - pd.Timedelta(days=_RETENTION_PERIOD)
    _supabase_client.table("idx_upcoming_dividend").delete().lt("payment_date", deletion_date.isoformat()).execute()

    logging.info(f"update {_LOCAL_TODAY} upcoming dividend data")
