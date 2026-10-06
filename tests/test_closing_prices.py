import unittest
from datetime import date
from types import SimpleNamespace
from scripts.update_market_data import closing_price_payload


class ClosingPriceTests(unittest.TestCase):
    def test_raw_close_and_actual_trade_date_are_preserved(self):
        instruments = [SimpleNamespace(stock_id="0050"), SimpleNamespace(stock_id="3498")]
        histories = {
            "0050": [SimpleNamespace(trade_date=date(2026, 10, 6), raw_close=115.95, adj_close=50)],
            "3498": [SimpleNamespace(trade_date=date(2026, 10, 5), raw_close=236.5, adj_close=100),
                     SimpleNamespace(trade_date=date(2026, 10, 6), raw_close=234.5, adj_close=99)],
        }
        payload = closing_price_payload(instruments, histories, date(2026, 10, 6), "2026-10-06T21:27:43+08:00")
        self.assertEqual(payload["rows"], [["0050", "2026-10-06", 115.95], ["3498", "2026-10-06", 234.5]])
        self.assertEqual(payload["priceSource"], "Yahoo Finance raw close")

    def test_future_or_invalid_closes_are_not_published_and_old_dates_are_not_relabelled(self):
        stocks = [SimpleNamespace(stock_id="3498"), SimpleNamespace(stock_id="6770")]
        histories = {
            "3498": [SimpleNamespace(trade_date=date(2026, 10, 5), raw_close=236.5),
                     SimpleNamespace(trade_date=date(2026, 10, 7), raw_close=250)],
            "6770": [SimpleNamespace(trade_date=date(2026, 10, 6), raw_close=float("nan"))],
        }
        payload = closing_price_payload(stocks, histories, date(2026, 10, 6), "now")
        self.assertEqual(payload["rows"], [["3498", "2026-10-05", 236.5]])
