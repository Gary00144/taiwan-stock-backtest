import json
import unittest
from datetime import date, datetime, timezone
from http.client import IncompleteRead
from unittest.mock import MagicMock, patch

from scripts import update_market_data as updater


class DownloadRecoveryTests(unittest.TestCase):
    @patch.object(updater.time, "sleep")
    def test_truncated_response_is_discarded_and_downloaded_again(self, sleep):
        response = MagicMock()
        response.__enter__.return_value = response
        response.read.side_effect = [IncompleteRead(b"partial", 100), b"complete"]
        with patch.object(updater, "urlopen", return_value=response) as request:
            self.assertEqual(updater.fetch_bytes("https://example.org/feed"), b"complete")
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once()

    @patch.object(updater.time, "sleep")
    def test_repeated_truncation_fails_without_returning_partial_data(self, sleep):
        response = MagicMock()
        response.__enter__.return_value = response
        response.read.side_effect = IncompleteRead(b"partial", 100)
        with patch.object(updater, "urlopen", return_value=response) as request:
            with self.assertRaisesRegex(RuntimeError, "after 4 attempts"):
                updater.fetch_bytes("https://example.org/feed")
        self.assertEqual(request.call_count, 4)
        self.assertEqual(sleep.call_count, 3)


class CompanySourceRecoveryTests(unittest.TestCase):
    def official_payload(self, url):
        if url == updater.TWSE_STOCK_MASTER:
            rows = [{"公司代號": str(i), "公司簡稱": f"上市{i}"} for i in range(1000, 1500)]
        elif url == updater.TPEX_STOCK_MASTER:
            rows = [
                {"SecuritiesCompanyCode": str(i), "CompanyAbbreviation": f"上櫃{i}"}
                for i in range(2000, 2500)
            ]
        else:
            raise RuntimeError("Name or service not known")
        return json.dumps(rows).encode()

    def test_mops_dns_outage_uses_both_official_api_schemas(self):
        with patch.object(updater, "fetch_bytes", side_effect=self.official_payload):
            instruments = updater.fetch_stock_master()
        by_id = {item.stock_id: item for item in instruments}
        self.assertEqual(len(by_id), 1000)
        self.assertEqual(by_id["1000"].stock_name, "上市1000")
        self.assertEqual(by_id["1000"].symbol, "1000.TW")
        self.assertEqual(by_id["2000"].stock_name, "上櫃2000")
        self.assertEqual(by_id["2000"].symbol, "2000.TWO")

    def test_empty_success_response_also_uses_fallback(self):
        def download(url):
            if url in (updater.MOPS_LISTED, updater.MOPS_OTC):
                return b"company_code,company_name\n"
            return self.official_payload(url)
        with patch.object(updater, "fetch_bytes", side_effect=download):
            self.assertEqual(len(updater.fetch_stock_master()), 1000)

    def test_unusable_sources_do_not_silently_shrink_universe(self):
        with patch.object(updater, "fetch_bytes", return_value=b"[]"):
            with self.assertRaisesRegex(RuntimeError, "All official company sources failed"):
                updater.fetch_stock_master()


class RecoveryDateTests(unittest.TestCase):
    def test_latest_closed_weekday_across_midnight_and_weekend(self):
        cases = [
            ("2026-10-03T00:08:00+08:00", "2026-10-02"),
            ("2026-10-02T00:08:00+08:00", "2026-10-01"),
            ("2026-10-02T14:19:00+08:00", "2026-10-01"),
            ("2026-10-02T14:20:00+08:00", "2026-10-02"),
            ("2026-10-04T18:00:00+08:00", "2026-10-02"),
            ("2026-10-05T09:00:00+08:00", "2026-10-02"),
            ("2026-10-05T14:20:00+08:00", "2026-10-05"),
            ("2026-10-02T16:08:00+00:00", "2026-10-02"),
        ]
        for now, expected in cases:
            with self.subTest(now=now):
                self.assertEqual(updater.default_target_date(datetime.fromisoformat(now)), date.fromisoformat(expected))

    def test_future_or_unclosed_override_cannot_publish_intraday_prices(self):
        with patch.object(updater, "default_target_date", return_value=date(2026, 10, 2)), \
             patch("sys.argv", ["updater", "--output", "/tmp/test-output", "--date", "2026-10-03"]), \
             patch.object(updater, "build_assets") as build:
            with self.assertRaisesRegex(SystemExit, "latest closed weekday"):
                updater.main()
            build.assert_not_called()


class TradingDayRecoveryTests(unittest.TestCase):
    def test_short_chart_delay_cannot_be_reported_as_a_holiday(self):
        older = updater.DailyBar(date(2026, 9, 30), 1, 1, 1, 1, 1)
        calendar = [{"Date": "1150925", "Name": "中秋節", "Description": "依規定放假1日。"}]
        with patch.object(updater, "fetch_instrument_history", return_value=([older], "yahoo-global")) as history, \
             patch.object(updater, "fetch_bytes", return_value=json.dumps(calendar).encode()):
            with self.assertRaisesRegex(RuntimeError, "retry instead of skipping"):
                updater.preflight_is_trading_day(date(2026, 10, 2))
            self.assertEqual(history.call_count, 2)

    def test_verified_holiday_skips_without_publishing(self):
        older = updater.DailyBar(date(2026, 9, 24), 1, 1, 1, 1, 1)
        calendar = [{"Date": "1150925", "Name": "中秋節", "Description": "依規定放假1日。"}]
        with patch.object(updater, "fetch_instrument_history", return_value=([older], "yahoo-global")), \
             patch.object(updater, "fetch_bytes", return_value=json.dumps(calendar).encode()):
            self.assertFalse(updater.preflight_is_trading_day(date(2026, 9, 25)))

    def test_open_day_calendar_entries_are_not_holidays(self):
        calendar = [{"Date": "1150102", "Name": "國曆新年開始交易日", "Description": "國曆新年開始交易。"}]
        with patch.object(updater, "fetch_bytes", return_value=json.dumps(calendar).encode()):
            self.assertFalse(updater.is_official_holiday(date(2026, 1, 2)))

    def test_calendar_for_another_year_does_not_confirm_a_holiday(self):
        calendar = [{"Date": "1150101", "Name": "元旦"}]
        with patch.object(updater, "fetch_bytes", return_value=json.dumps(calendar).encode()):
            with self.assertRaisesRegex(RuntimeError, "does not cover"):
                updater.is_official_holiday(date(2027, 1, 4))


class RecentQuoteRecoveryTests(unittest.TestCase):
    def test_delayed_global_close_uses_the_validated_taiwan_tail(self):
        old = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        overlap = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        new = updater.DailyBar(date(2026, 10, 2), 101, 101, 102, 102, 20)
        item = updater.Instrument("2330", "台積電", "TWSE", "2330.TW", "STOCK")
        with patch.object(updater, "fetch_global_history", return_value=[old]), \
             patch.object(updater, "fetch_yahoo_tw_adjusted", return_value=[overlap, new]):
            rows, source = updater.fetch_instrument_history(item, date(2026, 10, 2))
        self.assertEqual(rows[-1], new)
        self.assertEqual(source, "yahoo-global-with-tw-tail")

    def test_dividend_in_missing_days_rebases_the_entire_older_history(self):
        first = updater.DailyBar(date(2016, 1, 4), 50, 40, 50, 40, 10)
        old = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        local_old = updater.DailyBar(date(2026, 9, 30), 100, 95, 100, 95, 10)
        current = updater.DailyBar(date(2026, 10, 2), 95, 95, 96, 96, 20)
        rows = updater.merge_yahoo_tw_tail([first, old], [local_old, current])
        self.assertEqual(rows[0].adj_close, 38)
        self.assertEqual(rows[0].raw_close, 50)
        self.assertEqual(rows[1].adj_close, 95)
        self.assertEqual(rows[2].adj_close, 96)

    def test_price_basis_mismatch_is_rejected(self):
        old = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        mismatch = updater.DailyBar(date(2026, 9, 30), 25, 25, 25, 25, 10)
        with self.assertRaisesRegex(ValueError, "overlap disagrees"):
            updater.merge_yahoo_tw_tail([old], [mismatch])


class BatchedRecoveryTests(unittest.TestCase):
    def test_batch_response_is_matched_by_symbol_not_position(self):
        stamp = int(datetime(2026, 10, 2, 1, tzinfo=timezone.utc).timestamp())
        def entry(symbol, close):
            return {"symbol": symbol, "chart": {"timestamp": [stamp], "indicators": {"quote": [{"open": [close], "close": [close], "volume": [10]}]}}}
        payload = {"data": [entry("6488.TWO", 1190), entry("0050.TW", 112.8)]}
        with patch.object(updater, "fetch_bytes", return_value=json.dumps(payload).encode()):
            rows = updater.fetch_yahoo_tw_batch(["0050.TW", "6488.TWO"], date(2026, 10, 1), date(2026, 10, 2))
        self.assertEqual(rows["0050.TW"][0][2], 112.8)
        self.assertEqual(rows["6488.TWO"][0][2], 1190)

    @patch.object(updater.time, "sleep")
    def test_missing_recent_close_is_batched_without_unneeded_dividend_pages(self, sleep):
        item = updater.Instrument("2330", "台積電", "TWSE", "2330.TW", "STOCK")
        old = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        histories = {"2330": [old]}
        sources = {"2330": "yahoo-global"}
        raw = {"2330.TW": [(date(2026, 9, 30), 100, 100, 10), (date(2026, 10, 1), 101, 102, 10), (date(2026, 10, 2), 102, 103, 10)]}
        with patch.object(updater, "fetch_yahoo_tw_batch", return_value=raw) as batch, \
             patch.object(updater, "fetch_bytes") as dividend_page:
            failed = updater.repair_recent_histories([item], histories, sources, {}, date(2026, 10, 2))
        self.assertEqual(failed, [])
        self.assertEqual(histories["2330"][-1].adj_close, 103)
        self.assertEqual(sources["2330"], "yahoo-global-with-tw-tail")
        batch.assert_called_once()
        dividend_page.assert_not_called()

    @patch.object(updater.time, "sleep")
    def test_unavailable_tail_keeps_valid_history_and_reports_missing_price(self, sleep):
        item = updater.Instrument("2235", "停牌標的", "TPEx", "2235.TWO", "STOCK")
        old = updater.DailyBar(date(2026, 9, 24), 100, 100, 100, 100, 10)
        histories = {"2235": [old]}
        with patch.object(updater, "fetch_yahoo_tw_batch", return_value={}):
            failed = updater.repair_recent_histories([item], histories, {}, {}, date(2026, 10, 2))
        self.assertEqual(histories["2235"], [old])
        self.assertEqual(failed[0]["stockId"], "2235")
        self.assertFalse(any(row.trade_date == date(2026, 10, 2) for row in histories["2235"]))

    @patch.object(updater.time, "sleep")
    def test_dividend_event_requires_metadata_and_rebases_older_history(self, sleep):
        item = updater.Instrument("2330", "台積電", "TWSE", "2330.TW", "STOCK")
        old = updater.DailyBar(date(2026, 9, 30), 100, 100, 100, 100, 10)
        histories = {"2330": [old]}
        raw = {"2330.TW": [(date(2026, 9, 30), 100, 100, 10), (date(2026, 10, 2), 95, 96, 10)]}
        stamp = int(datetime(2026, 10, 2, 1, tzinfo=timezone.utc).timestamp())
        events = {"2330": {"dividends": {str(stamp): {"date": stamp, "amount": 5}}}}
        with patch.object(updater, "fetch_yahoo_tw_batch", return_value=raw), \
             patch.object(updater, "fetch_bytes", return_value=b"dividend metadata") as page, \
             patch.object(updater, "parse_yahoo_tw_dividends", return_value=[(date(2026, 10, 2), 0.95)]):
            failed = updater.repair_recent_histories([item], histories, {}, events, date(2026, 10, 2))
        self.assertEqual(failed, [])
        self.assertEqual(histories["2330"][0].adj_close, 95)
        self.assertEqual(histories["2330"][-1].adj_close, 96)
        page.assert_called_once()


if __name__ == "__main__":
    unittest.main()
