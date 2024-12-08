import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client, Client
import time

load_dotenv()


class DividendChecker:
    def __init__(self, supabase_client : Client, last_n_day=7):
        self.url = "https://sahamidx.com/?view=Stock.Cash.Dividend&path=Stock&field_sort=rg_ng_ex_date&sort_by=DESC&page=1"
        self.supabase_client = supabase_client
        self.start_date = (pd.Timestamp.now("Asia/Bangkok") - pd.Timedelta(days=last_n_day - 1)).strftime("%Y-%m-%d")
        self.end_date = pd.Timestamp.now("Asia/Bangkok").strftime("%Y-%m-%d")
        self.retrieved_records = []
        self.allowed_symbols = [k['symbol'][:4] for k in self.supabase_client.from_("idx_company_profile").select("symbol").execute().data]

    def get_dividend_records(self):
        attempt = 1
        max_attempt = 10
        while(attempt <= max_attempt):
          try:
            response = requests.get(self.url)
            if response.status_code != 200:
                raise Exception("Error retrieving data from SahamIDX")

            soup = BeautifulSoup(response.text, "lxml")
            table = soup.find("table", {"class": "tbl_border_gray"})
            rows = table.find_all("tr", recursive=False)[1:]
            # parameter to iterate the rows
            num_of_rows = len(rows)
            counter = num_of_rows - 1
            # stack to store the rows
            stack = []
            while(True):
                row = rows[counter]
                # push into stack
                stack.append(row)
                if len(row.find_all("td")) > 2:
                    values : str = row.find_all("td")[0].text.strip()
                    # can value typecast to int?
                    try:
                        int(values)
                        
                        #invert the stack
                        stack = stack[::-1]
                        # get first element
                        first_element = stack[0]
                        # get as td
                        first_element_td = first_element.find_all("td")
                        # get symbol
                        symbol = first_element_td[1].find("a").text.strip()
                            
                        # get dividend
                        dividend = first_element_td[3].text.strip()
                        # get date
                        date = datetime.strptime(first_element_td[6].text.strip(), "%d-%b-%Y").strftime(
                            "%Y-%m-%d")

                        if (symbol in self.allowed_symbols) and(self.start_date <= date <= self.end_date):
                            data_dict = {
                                "symbol": symbol + ".JK",
                                "date": date,
                                "dividend": dividend,
                                "updated_on": pd.Timestamp.now(tz="GMT").strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            print(data_dict)
                            self.retrieved_records.append(data_dict)
                    except:
                        pass
                    #clear the stack
                    stack = []
                # reduce counter
                counter -= 1
                if(counter < 0):
                    break
            
            # Break the attempt while if success
            break
          except Exception as e:
            if (attempt == max_attempt):
              print(f"\t[ATTEMPTS FAILED] Failed after {max_attempt} attempts. | {e}")

            else:
              print(f"\t[FAILED] Failed after {attempt} attempt(s). Retrying after 2 seconds... | {e}")
              time.sleep(2)
            attempt += 1

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
    # stock_split_checker.upsert_to_db()
