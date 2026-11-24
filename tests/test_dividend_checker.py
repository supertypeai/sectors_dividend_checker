"""Read-only tests for the dividend scrapers.

No network and no database: ``requests.get`` is faked and the Supabase client is
replaced by FakeClient, which records upsert payloads and raises on insert,
update and delete. If the pipeline ever tries to write through a path other than
upsert, these tests fail loudly instead of touching the live DB.
"""

import os
import pathlib
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy-key")

import supabase  # noqa: E402


class WriteAttempted(AssertionError):
    """Raised when the code under test tries to insert/update/delete."""


class FakeResult:
    def __init__(self, data=None, count=None):
        self.data = [] if data is None else data
        self.count = len(self.data) if count is None else count


class FakeSelect:
    def __init__(self, client, table):
        self.client = client
        self.table = table

    def select(self, *args, **kwargs):
        return self

    def eq(self, *args, **kwargs):
        return self

    def order(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def execute(self):
        return FakeResult(self.client.select_data.get(self.table, []))


class FakeTable:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def select(self, *args, **kwargs):
        return FakeSelect(self.client, self.name)

    def upsert(self, data, on_conflict=None):
        self._pending = {
            "table": self.name, "data": list(data), "on_conflict": on_conflict
        }
        return self

    def execute(self):
        if getattr(self, "_pending", None):
            self.client.upserts.append(self._pending)
            self._pending = None
        return FakeResult([])

    def insert(self, data):
        raise WriteAttempted("insert is not expected in these tests")

    def update(self, data):
        raise WriteAttempted("update is not expected in these tests")

    def delete(self):
        raise WriteAttempted("delete is not expected in these tests")


class FakeClient:
    def __init__(self):
        self.upserts = []
        self.select_data = {
            "idx_company_profile": [{"symbol": "ABCD.JK"}],
            "idx_dividend": [],
        }

    def from_(self, table):
        return FakeTable(self, table)

    def table(self, table):
        return FakeTable(self, table)


_FAKE = FakeClient()
# Patch before importing the future checker: it builds a client at import time.
supabase.create_client = lambda *args, **kwargs: _FAKE

import dividend_checker  # noqa: E402
import future_dividend_checker  # noqa: E402


@pytest.fixture(autouse=True, scope="session")
def no_network():
    """Hard guarantee: any attempt to open a socket fails the test run."""
    import socket

    original = socket.socket

    def blocked(*args, **kwargs):
        raise RuntimeError("network access is disabled in these tests")

    socket.socket = blocked
    yield
    socket.socket = original


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(dividend_checker.time, "sleep", lambda *_: None)
    monkeypatch.setattr(future_dividend_checker.time, "sleep", lambda *_: None)


class Resp:
    def __init__(self, text, status=200):
        self.text = text
        self.status_code = status


ROW = """<table><tr>
<td data-header="Nama">ABCD</td>
<td data-header="Amount">10</td>
<td data-header="Ex Date">15-Nov-2026</td>
<td data-header="Payment Date">20-Nov-2026</td>
<td data-header="Recording Date">13-Nov-2026</td>
<td data-header="Cum Date">12-Nov-2026</td>
</tr></table>"""
OLD_ROW = ROW.replace("15-Nov-2026", "01-Jan-2026")
EMPTY = "<table></table>"


def make_checker():
    checker = dividend_checker.DividendChecker(_FAKE)
    checker.start_date = "2026-11-01"
    checker.end_date = "2026-11-30"
    _FAKE.upserts.clear()
    return checker


# --------------------------------------------------------------------- scraping
def test_page_one_empty_raises(monkeypatch):
    checker = make_checker()
    monkeypatch.setattr(dividend_checker.requests, "get", lambda *a, **k: Resp(EMPTY))

    with pytest.raises(RuntimeError, match="Page 1 returned no dividend rows"):
        checker.get_dividend_records()


def test_retry_does_not_duplicate_accumulated_records(monkeypatch):
    checker = make_checker()
    calls = {"n": 0}

    def fake_get(url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return Resp(ROW)          # attempt 1, page 1: one dividend
        if calls["n"] == 2:
            return Resp("", status=500)  # attempt 1, page 2: blows up -> retry
        if int(url.rsplit("page/", 1)[1]) == 1:
            return Resp(ROW)          # attempt 2, page 1: same dividend again
        return Resp(EMPTY)            # attempt 2, page 2: end of listing

    monkeypatch.setattr(dividend_checker.requests, "get", fake_get)

    checker.get_dividend_records()

    assert len(checker.retrieved_records) == 1
    assert checker.retrieved_records[0]["symbol"] == "ABCD.JK"


def test_empty_window_is_not_an_error(monkeypatch):
    checker = make_checker()
    monkeypatch.setattr(dividend_checker.requests, "get", lambda *a, **k: Resp(OLD_ROW))

    checker.get_dividend_records()

    assert checker.retrieved_records == []


def test_page_with_only_unlisted_symbols_is_not_an_error(monkeypatch):
    """Rows exist but all belong to symbols outside idx_company_profile."""
    checker = make_checker()
    monkeypatch.setattr(
        dividend_checker.requests,
        "get",
        lambda url, **k: Resp(ROW.replace(">ABCD<", ">ZZZZ<"))
        if int(url.rsplit("page/", 1)[1]) == 1
        else Resp(EMPTY),
    )

    checker.get_dividend_records()

    assert checker.retrieved_records == []
    assert checker.failed_records == []


def test_all_attempts_exhausted_raises(monkeypatch):
    checker = make_checker()
    monkeypatch.setattr(
        dividend_checker.requests, "get", lambda *a, **k: Resp("", status=503)
    )

    with pytest.raises(RuntimeError, match="Failed to fetch dividends after 3 attempts"):
        checker.get_dividend_records()


# ---------------------------------------------------------------------- upserts
def test_upsert_dedupes_on_symbol_date():
    checker = make_checker()
    checker.retrieved_records = [
        {"symbol": "ABCD.JK", "date": "2026-11-15", "dividend": 10},
        {"symbol": "ABCD.JK", "date": "2026-11-15", "dividend": 12},
        {"symbol": "ABCD.JK", "date": "2026-11-20", "dividend": 7},
    ]

    checker.upsert_to_db()

    assert len(_FAKE.upserts) == 1
    payload = _FAKE.upserts[0]
    assert payload["table"] == "idx_dividend"
    assert payload["on_conflict"] == "symbol,date"
    assert len(payload["data"]) == 2
    kept = [row["dividend"] for row in payload["data"] if row["date"] == "2026-11-15"]
    assert kept == [12], "last record for a duplicate key must win"


def test_upsert_collapses_three_identical_keys():
    checker = make_checker()
    checker.retrieved_records = [
        {"symbol": "ABCD.JK", "date": "2026-11-15", "dividend": value}
        for value in (5, 10, 15)
    ]

    checker.upsert_to_db()

    payload = _FAKE.upserts[0]
    assert len(payload["data"]) == 1
    assert payload["data"][0]["dividend"] == 15


def test_future_upsert_dedupes_on_symbol_payment_date():
    checker = future_dividend_checker.FutureDividendChecker(_FAKE)
    _FAKE.upserts.clear()
    checker.retrieved_records = [
        {"symbol": "ABCD.JK", "ex_date": "2026-11-15", "payment_date": "2026-11-20"},
        {"symbol": "ABCD.JK", "ex_date": "2026-11-16", "payment_date": "2026-11-20"},
    ]

    checker.upsert_to_db()

    payload = _FAKE.upserts[0]
    assert payload["table"] == "idx_upcoming_dividend"
    assert payload["on_conflict"] == "symbol,payment_date"
    assert len(payload["data"]) == 1


def test_future_checker_refetches_empty_window_three_times(monkeypatch):
    calls = {"n": 0}

    def empty_scrape(self, include_payment_date=True):
        calls["n"] += 1
        self.retrieved_records = []

    monkeypatch.setattr(
        dividend_checker.DividendChecker, "get_dividend_records", empty_scrape
    )
    checker = future_dividend_checker.FutureDividendChecker(_FAKE)

    checker.get_dividend_records()

    assert calls["n"] == 3


def test_future_checker_stops_refetching_when_rows_arrive(monkeypatch):
    calls = {"n": 0}

    def one_row_then_empty(self, include_payment_date=True):
        calls["n"] += 1
        self.retrieved_records = (
            [{"symbol": "ABCD.JK", "date": "2026-11-15", "dividend": 10,
              "dividend_original": 10, "payment_date": "2026-11-20"}]
            if calls["n"] == 1 else []
        )

    monkeypatch.setattr(
        dividend_checker.DividendChecker, "get_dividend_records", one_row_then_empty
    )
    checker = future_dividend_checker.FutureDividendChecker(_FAKE)

    checker.get_dividend_records()

    assert calls["n"] == 1
    assert checker.retrieved_records[0]["ex_date"] == "2026-11-15"
    assert checker.retrieved_records[0]["dividend_amount"] == 10


def test_future_checker_recovers_on_second_attempt(monkeypatch):
    calls = {"n": 0}

    def empty_then_row(self, include_payment_date=True):
        calls["n"] += 1
        self.retrieved_records = (
            []
            if calls["n"] == 1
            else [{"symbol": "ABCD.JK", "date": "2026-11-15", "dividend": 10,
                   "dividend_original": 10, "payment_date": "2026-11-20"}]
        )

    monkeypatch.setattr(
        dividend_checker.DividendChecker, "get_dividend_records", empty_then_row
    )
    checker = future_dividend_checker.FutureDividendChecker(_FAKE)

    checker.get_dividend_records()

    assert calls["n"] == 2
    assert checker.retrieved_records[0]["ex_date"] == "2026-11-15"


def test_future_checker_propagates_broken_source(monkeypatch):
    """A broken page 1 must fail the run, not be mistaken for an empty window."""

    def broken(self, include_payment_date=True):
        raise RuntimeError(
            "Page 1 returned no dividend rows; source layout changed or the "
            "request was blocked"
        )

    monkeypatch.setattr(
        dividend_checker.DividendChecker, "get_dividend_records", broken
    )
    checker = future_dividend_checker.FutureDividendChecker(_FAKE)

    with pytest.raises(RuntimeError, match="Page 1 returned no dividend rows"):
        checker.get_dividend_records()


# ------------------------------------------------------------------ fx helpers
def test_marked_currency_amount_is_converted(monkeypatch):
    monkeypatch.setattr(
        dividend_checker,
        "get_conversion_rates",
        lambda: {"USD": {"IDR": 16000.0, "SGD": 1.35}},
    )
    assert dividend_checker.parse_dividend_amount("US$ 0.5") == pytest.approx(8000.0)


def test_zero_amount_is_rejected(monkeypatch):
    monkeypatch.setattr(
        dividend_checker, "get_conversion_rates", lambda: {"USD": {"IDR": 16000.0}}
    )
    with pytest.raises(ValueError):
        dividend_checker.parse_dividend_amount("US$ 0")


# ------------------------------------------------------- read-only enforcement
def test_fake_client_blocks_writes():
    for blocked in (
        lambda: _FAKE.table("idx_dividend").insert({"a": 1}),
        lambda: _FAKE.table("idx_dividend").update({"a": 1}),
        lambda: _FAKE.table("idx_dividend").delete(),
    ):
        with pytest.raises(WriteAttempted):
            blocked()
