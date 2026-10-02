import json
import unittest
from datetime import date, datetime
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


if __name__ == "__main__":
    unittest.main()
