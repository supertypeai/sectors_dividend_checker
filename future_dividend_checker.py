import os
import requests

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from bs4 import BeautifulSoup, Tag
from supabase import create_client

# Default next N days of dividend ex-date to fetch
_DEFAULT_TIMEFRAME = 14  # days
# Default retention period after dividend payment date in database
_RETENTION_PERIOD = 14  # days
_LOCAL_TIMEZONE = 'Asia/Bangkok'
_LOCAL_TODAY = datetime.now(tz=ZoneInfo(_LOCAL_TIMEZONE)).date()

load_dotenv()

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_KEY")
_supabase_client = create_client(_SUPABASE_URL, _SUPABASE_KEY)

_UPCOMING_DIVIDEND_URL = "https://www.investing.com/dividends-calendar/Service/getCalendarFilteredData"
_UPCOMING_DIVIDEND_HEADERS = {
    "Host": "www.investing.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.investing.com",
    "Referer": "https://www.investing.com/dividends-calendar/",
    "X-Requested-With": "XMLHttpRequest",
}
# _UPCOMING_DIVIDEND_BODY = {
#     "&country[]": 48,
#     "currentTab": "thisWeek",
#     "limit_from": 0,
# }
_UPCOMING_DIVIDEND_BODY = r"&country%5B%5D=48&dateFrom={start_date}&dateTo={end_date}&submitFilters=1&limit_from=0"


def parse_en_us_datetime(date_string: str):
    return datetime.strptime(date_string, "%b %d, %Y")


def get_upcoming_dividend_records(timeframe: int = _DEFAULT_TIMEFRAME):
    start_date = _LOCAL_TODAY
    end_date = start_date + timedelta(days=timeframe)

    retries = 1
    while retries > 0:
        try:
            response = requests.post(
                _UPCOMING_DIVIDEND_URL,
                headers=_UPCOMING_DIVIDEND_HEADERS,
                json=_UPCOMING_DIVIDEND_BODY.format(start_date=start_date.isoformat(), end_date=end_date.isoformat()),
            )
            response.raise_for_status()

            json_response = response.json()
            table = json_response["data"]

            def exclude_tabledivider_tag(tag: Tag):
                # filter out date divider entries
                return tag.name == "tr" and not tag.has_attr("tablesorterdivider")

            soup = BeautifulSoup(table, features="lxml")
            rows = soup.find_all(exclude_tabledivider_tag)

            parsed_data = []
            for row in rows:
                cols = row.find_all_next("td")

                # Parse the data
                raw_symbol = cols[1].find_next("a").getText()
                ex_date = parse_en_us_datetime(cols[2].getText())
                payment_date = parse_en_us_datetime(cols[5].getText())
                dividend_amount = cols[3].getText()

                # Convert into dictionary for Supabase client format
                data_dict = {
                    "symbol": f"{raw_symbol}.JK",
                    "ex_date": ex_date.isoformat(),
                    "payment_date": payment_date.isoformat(),
                    "dividend_amount": dividend_amount,
                    "updated_on": datetime.now(tz=timezone.utc).isoformat()
                }

                # Append the data into list
                parsed_data.append(data_dict)

            return parsed_data

        except requests.HTTPError as e:
            retries -= 1
            print(f"failed to fetch data from {_UPCOMING_DIVIDEND_URL}: {e}")


if __name__ == "__main__":
    # Update upcoming dividend data
    upcoming_dividend_data = get_upcoming_dividend_records()
    _supabase_client.table("idx_upcoming_dividend").upsert(upcoming_dividend_data, ignore_duplicates=False).execute()

    # Delete past dividend data from the same table
    deletion_date = _LOCAL_TODAY - timedelta(days=_RETENTION_PERIOD)
    _supabase_client.table("idx_upcoming_dividend").delete().lt("payment_date", deletion_date.isoformat()).execute()
