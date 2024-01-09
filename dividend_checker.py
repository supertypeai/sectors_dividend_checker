import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()


class DividendChecker:
    def __init__(self, supabase_client, last_n_day=7):
        self.url = "https://sahamidx.com/?view=Stock.Cash.Dividend&path=Stock&field_sort=rg_ng_ex_date&sort_by=DESC&page=1"
        self.supabase_client = supabase_client
        self.start_date = (pd.Timestamp.now("Asia/Bangkok") - pd.Timedelta(days=last_n_day - 1)).strftime("%Y-%m-%d")
        self.end_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        self.retrieved_records = []

    def get_dividend_records(self):
        response = requests.get(self.url)
        if response.status_code != 200:
            raise Exception("Error retrieving data from SahamIDX")

        soup = BeautifulSoup(response.text, "lxml")
        table = soup.find("table", {"class": "tbl_border_gray"})
        rows = table.find_all("tr", recursive=False)[1:]

        for row in rows:
            if len(row.find_all("td")) > 2:
                values = row.find_all("td")
                date = datetime.strptime(values[6].text.strip(), "%d-%b-%Y").strftime(
                    "%Y-%m-%d"
                )
                if self.start_date <= date <= self.end_date:
                    dividend = float(values[3].text.strip().replace(",", ""))
                    data_dict = {
                        "symbol": values[1].find("a").text.strip() + ".JK",
                        "date": date,
                        "dividend": round(dividend, 5),
                        "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    self.retrieved_records.append(data_dict)

    def upsert_to_db(self):
        if not self.retrieved_records:
            print("No records to upsert to database. All data is up to date")
            raise SystemExit(0)

        try:
            self.supabase_client.table("idx_dividend").upsert(
                self.retrieved_records
            ).execute()
            print(
                f"Successfully upserted {len(self.retrieved_records)} data to database"
            )
        except Exception as e:
            raise Exception(f"Error upserting to database: {e}")


if __name__ == "__main__":
    url, key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    supabase_client = create_client(url, key)

    stock_split_checker = DividendChecker(supabase_client)
    stock_split_checker.get_dividend_records()
    stock_split_checker.upsert_to_db()
