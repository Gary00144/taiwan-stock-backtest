#!/usr/bin/env python3
"""Build the market-data snapshot consumed by taiwan-stock-backtest.

The job is intentionally self-contained (Python standard library only) so it can
run on GitHub-hosted runners without a dependency install.  It fetches the
current TWSE/TPEx instrument universe, downloads Yahoo adjusted daily history,
validates that the current Taiwan trading day is sufficiently complete, and
writes deterministic gzip JSON assets.

No output is published by this script.  The GitHub Actions workflow publishes
the generated directory only after this process exits successfully.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import json
import math
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
START_DATE = date(2016, 1, 1)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36"
)

MOPS_LISTED = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
MOPS_OTC = "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"
TWSE_DAILY = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_DAILY = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
YAHOO_GLOBAL = "https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_TW_CHART = (
    "https://tw.stock.yahoo.com/_td-stock/api/resource/"
    "FinanceChartService.ApacLibraCharts"
)
YAHOO_TW_DIVIDEND = "https://tw.stock.yahoo.com/quote/{symbol}/dividend"

MIN_STOCKS = 1900
MIN_ETFS = 300
MIN_TODAY_COVERAGE = 0.90
REQUEST_ATTEMPTS = 5


@dataclass(frozen=True)
class Instrument:
    stock_id: str
    stock_name: str
    market: str
    symbol: str
    instrument_type: str


@dataclass(frozen=True)
class DailyBar:
    trade_date: date
    raw_open: float
    adj_open: float
    raw_close: float
    adj_close: float
    volume: int


def finite_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def fetch_bytes(url: str, *, timeout: int = 60, attempts: int = 4) -> bytes:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/csv,text/html,*/*",
                    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
                },
            )
            with urlopen(request, timeout=timeout) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(min(2**attempt + random.random(), 10))
    raise RuntimeError(f"request failed after {attempts} attempts: {url}: {last_error}")


def decode_csv(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def fetch_stock_master() -> list[Instrument]:
    records: list[Instrument] = []
    for market, suffix, url in (
        ("TWSE", ".TW", MOPS_LISTED),
        ("TPEx", ".TWO", MOPS_OTC),
    ):
        reader = csv.DictReader(decode_csv(fetch_bytes(url)).splitlines())
        for row in reader:
            stock_id = str(row.get("公司代號", "")).strip()
            stock_name = str(row.get("公司簡稱", "")).strip()
            if not (len(stock_id) == 4 and stock_id.isdigit() and stock_name):
                continue
            records.append(
                Instrument(stock_id, stock_name, market, f"{stock_id}{suffix}", "STOCK")
            )
    unique = {record.stock_id: record for record in records}
    return sorted(unique.values(), key=lambda row: (row.market, row.stock_id))


def fetch_etf_master() -> list[Instrument]:
    records: list[Instrument] = []
    sources = (
        ("TWSE", ".TW", TWSE_DAILY, "Code", "Name"),
        ("TPEx", ".TWO", TPEX_DAILY, "SecuritiesCompanyCode", "CompanyName"),
    )
    for market, suffix, url, id_key, name_key in sources:
        payload = json.loads(fetch_bytes(url))
        for row in payload:
            stock_id = str(row.get(id_key, "")).strip().upper()
            stock_name = str(row.get(name_key, "")).strip()
            if (
                not re.fullmatch(r"00[0-9A-Z]+", stock_id)
                or not 4 <= len(stock_id) <= 6
                or not stock_name
            ):
                continue
            records.append(
                Instrument(stock_id, stock_name, market, f"{stock_id}{suffix}", "ETF")
            )
    unique = {record.stock_id: record for record in records}
    return sorted(unique.values(), key=lambda row: (row.market, row.stock_id))


def epoch_start(value: date) -> int:
    return int(datetime.combine(value, datetime_time.min, timezone.utc).timestamp())


def yahoo_url(symbol: str, start: date, end_inclusive: date, host: str) -> str:
    return (
        YAHOO_GLOBAL.format(host=host, symbol=quote(symbol))
        + f"?period1={epoch_start(start)}"
        + f"&period2={epoch_start(end_inclusive + timedelta(days=1))}"
        + "&interval=1d&events=div%2Csplits&includeAdjustedClose=true"
    )


def parse_yahoo_global(content: bytes, start: date, end_inclusive: date) -> list[DailyBar]:
    payload = json.loads(content)
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise ValueError(str(chart["error"]))
    results = chart.get("result") or []
    if not results:
        raise ValueError("Yahoo chart result is empty")
    result = results[0]
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quotes = indicators.get("quote") or []
    adjusted = indicators.get("adjclose") or []
    quote_data = quotes[0] if quotes else {}
    adj_closes = adjusted[0].get("adjclose", []) if adjusted else []
    opens = quote_data.get("open") or []
    closes = quote_data.get("close") or []
    volumes = quote_data.get("volume") or []

    output: list[DailyBar] = []
    for index, timestamp in enumerate(timestamps):
        raw_open = finite_number(opens[index] if index < len(opens) else None)
        raw_close = finite_number(closes[index] if index < len(closes) else None)
        adj_close = finite_number(adj_closes[index] if index < len(adj_closes) else None)
        volume = finite_number(volumes[index] if index < len(volumes) else None) or 0.0
        if raw_open is None or raw_open <= 0 or raw_close is None or raw_close <= 0:
            continue
        adj_close = adj_close if adj_close is not None and adj_close > 0 else raw_close
        trade_date = datetime.fromtimestamp(int(timestamp), TAIPEI).date()
        if not start <= trade_date <= end_inclusive:
            continue
        factor = adj_close / raw_close
        output.append(
            DailyBar(
                trade_date=trade_date,
                raw_open=round(raw_open, 6),
                adj_open=round(raw_open * factor, 6),
                raw_close=round(raw_close, 6),
                adj_close=round(adj_close, 6),
                volume=max(0, round(volume)),
            )
        )
    if not output:
        raise ValueError("Yahoo chart returned no valid rows")
    return sorted(output, key=lambda row: row.trade_date)


def yahoo_tw_chart_url(symbol: str, ticks: int = 800) -> str:
    encoded_symbols = quote(json.dumps([symbol], separators=(",", ":")))
    return (
        f"{YAHOO_TW_CHART};symbols={encoded_symbols};period=d;numOfTicks={ticks};type=chart"
        "?device=desktop&intl=tw&lang=zh-Hant-TW&partner=none"
        "&region=TW&returnMeta=true&site=finance&tz=Asia%2FTaipei"
    )


def parse_yahoo_tw_raw(content: bytes, start: date, end_inclusive: date) -> list[tuple[date, float, float, int]]:
    payload = json.loads(content)
    data = payload.get("data") or []
    if not data:
        raise ValueError("Yahoo Taiwan chart result is empty")
    chart = data[0].get("chart") or {}
    timestamps = chart.get("timestamp") or []
    quotes = (chart.get("indicators") or {}).get("quote") or []
    quote_data = quotes[0] if quotes else {}
    opens = quote_data.get("open") or []
    closes = quote_data.get("close") or []
    volumes = quote_data.get("volume") or []
    output: list[tuple[date, float, float, int]] = []
    for index, timestamp in enumerate(timestamps):
        raw_open = finite_number(opens[index] if index < len(opens) else None)
        raw_close = finite_number(closes[index] if index < len(closes) else None)
        volume = finite_number(volumes[index] if index < len(volumes) else None) or 0.0
        if raw_open is None or raw_open <= 0 or raw_close is None or raw_close <= 0:
            continue
        trade_date = datetime.fromtimestamp(int(timestamp), TAIPEI).date()
        if start <= trade_date <= end_inclusive:
            # Yahoo Taiwan reports share volume in thousands.
            output.append((trade_date, raw_open, raw_close, max(0, round(volume * 1000))))
    return sorted(output, key=lambda row: row[0])


def parse_yahoo_tw_dividends(content: bytes, end_inclusive: date) -> list[tuple[date, float]]:
    text = content.decode("utf-8", errors="replace")
    marker = '"QuoteDividendStore":'
    marker_index = text.find(marker)
    if marker_index < 0:
        return []
    object_index = text.find("{", marker_index + len(marker))
    if object_index < 0:
        return []
    store, _ = json.JSONDecoder().raw_decode(text[object_index:])
    events: dict[date, float] = {}
    for key, value in store.items():
        if not key.startswith("dividend-") or not isinstance(value, dict):
            continue
        dividends = ((value.get("data") or {}).get("dividends") or [])
        for item in dividends:
            if item.get("recordType") != "SUB" or item.get("isUpcoming"):
                continue
            ex_dividend = item.get("exDividend") or {}
            cash = finite_number(ex_dividend.get("cash"))
            previous_close = finite_number((item.get("exDatePreviousClose") or {}).get("raw"))
            raw_date = str(item.get("exDate") or ex_dividend.get("date") or "")[:10]
            try:
                ex_date = date.fromisoformat(raw_date)
            except ValueError:
                continue
            if (
                cash is None
                or cash <= 0
                or previous_close is None
                or previous_close <= cash
                or ex_date > end_inclusive
            ):
                continue
            events[ex_date] = (previous_close - cash) / previous_close
    return sorted(events.items())


def fetch_yahoo_tw_adjusted(symbol: str, start: date, end_inclusive: date) -> list[DailyBar]:
    raw_rows = parse_yahoo_tw_raw(fetch_bytes(yahoo_tw_chart_url(symbol)), start, end_inclusive)
    if not raw_rows:
        raise ValueError("Yahoo Taiwan fallback returned no rows")
    dividend_events = parse_yahoo_tw_dividends(
        fetch_bytes(YAHOO_TW_DIVIDEND.format(symbol=quote(symbol))),
        end_inclusive,
    )
    output: list[DailyBar] = []
    for trade_date, raw_open, raw_close, volume in raw_rows:
        factor = math.prod(value for ex_date, value in dividend_events if ex_date > trade_date)
        output.append(
            DailyBar(
                trade_date=trade_date,
                raw_open=round(raw_open, 6),
                adj_open=round(raw_open * factor, 6),
                raw_close=round(raw_close, 6),
                adj_close=round(raw_close * factor, 6),
                volume=volume,
            )
        )
    return output


def fetch_global_history(symbol: str, start: date, end_inclusive: date) -> list[DailyBar]:
    last_error = ""
    hosts = ("query1", "query2")
    for attempt in range(REQUEST_ATTEMPTS):
        host = hosts[attempt % len(hosts)]
        try:
            content = fetch_bytes(
                yahoo_url(symbol, start, end_inclusive, host),
                timeout=75,
                attempts=1,
            )
            return parse_yahoo_global(content, start, end_inclusive)
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if attempt + 1 < REQUEST_ATTEMPTS:
                time.sleep(min((1.5, 3.0, 6.0, 12.0)[min(attempt, 3)] + random.random(), 14))
    raise RuntimeError(last_error or "Yahoo global history failed")


def fetch_instrument_history(instrument: Instrument, end_inclusive: date) -> tuple[list[DailyBar], str]:
    # Yahoo Global currently has an incomplete historical start date for 00972.
    # Yahoo Taiwan contains its full local history; reconstruct adjusted prices
    # from the local price/dividend records rather than silently dropping it.
    if instrument.stock_id == "00972":
        rows = fetch_yahoo_tw_adjusted(instrument.symbol, START_DATE, end_inclusive)
        return rows, "yahoo-tw-adjusted"
    rows = fetch_global_history(instrument.symbol, START_DATE, end_inclusive)
    return rows, "yahoo-global"


def preflight_is_trading_day(today: date) -> bool:
    start = today - timedelta(days=3)
    hits = 0
    errors: list[str] = []
    for symbol in ("0050.TW", "2330.TW"):
        try:
            rows = fetch_global_history(symbol, start, today)
            if any(row.trade_date == today for row in rows):
                hits += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{symbol}: {exc}")
    if hits == 0 and not errors:
        return False
    if hits == 0 and errors:
        raise RuntimeError("Yahoo preflight failed: " + "; ".join(errors))
    if hits != 2:
        raise RuntimeError(
            f"Yahoo preflight is incomplete for {today.isoformat()}: {hits}/2 anchors have today's price"
        )
    return True


def weekdays_in_month_until(value: date) -> int:
    return sum(
        1
        for day in range(1, value.day + 1)
        if date(value.year, value.month, day).weekday() < 5
    )


def monthly_features(rows: list[DailyBar], end_inclusive: date) -> list[list[Any]]:
    grouped: dict[str, list[DailyBar]] = {}
    for row in rows:
        grouped.setdefault(row.trade_date.strftime("%Y-%m"), []).append(row)
    output: list[list[Any]] = []
    history_days = 0
    for month in sorted(grouped):
        month_rows = sorted(grouped[month], key=lambda row: row.trade_date)
        first = month_rows[0]
        last = month_rows[-1]
        if last.trade_date.year == end_inclusive.year and last.trade_date.month == end_inclusive.month:
            period_end = end_inclusive
        else:
            last_day = calendar.monthrange(last.trade_date.year, last.trade_date.month)[1]
            period_end = date(last.trade_date.year, last.trade_date.month, last_day)
        weekdays = weekdays_in_month_until(period_end)
        history_days += weekdays
        total_volume = sum(row.volume for row in month_rows)
        turnover = last.raw_close * total_volume / max(1, weekdays)
        month_end_factor = last.adj_close / last.raw_close
        first_adj_open = first.raw_open * month_end_factor
        output.append(
            [
                month,
                first.trade_date.isoformat(),
                round(first_adj_open, 6),
                last.trade_date.isoformat(),
                last.adj_close,
                last.raw_close,
                round(turnover),
                history_days,
            ]
        )
    return output


def write_gzip_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    with gzip.GzipFile(filename=str(path), mode="wb", compresslevel=9, mtime=0) as handle:
        handle.write(encoded)


def build_assets(output_dir: Path, workers: int, today: date) -> None:
    if not preflight_is_trading_day(today):
        print(f"SKIP: {today.isoformat()} is not a Yahoo Taiwan trading day")
        return

    stocks = fetch_stock_master()
    etfs = fetch_etf_master()
    if len(stocks) < MIN_STOCKS or len(etfs) < MIN_ETFS:
        raise RuntimeError(
            f"official master list is unexpectedly small: stocks={len(stocks)} etfs={len(etfs)}"
        )
    instruments = [*stocks, *etfs]
    print(f"Master list: stocks={len(stocks)} etfs={len(etfs)} total={len(instruments)}", flush=True)

    histories: dict[str, list[DailyBar]] = {}
    sources: dict[str, str] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(fetch_instrument_history, instrument, today): instrument
            for instrument in instruments
        }
        finished = 0
        for future in as_completed(futures):
            instrument = futures[future]
            try:
                rows, source = future.result()
                histories[instrument.stock_id] = rows
                sources[instrument.stock_id] = source
            except Exception as exc:  # noqa: BLE001
                failures.append(
                    {
                        "stockId": instrument.stock_id,
                        "stockName": instrument.stock_name,
                        "symbol": instrument.symbol,
                        "error": str(exc)[:500],
                    }
                )
            finished += 1
            if finished % 100 == 0 or finished == len(instruments):
                print(
                    f"Yahoo: {finished}/{len(instruments)} success={len(histories)} failed={len(failures)}",
                    flush=True,
                )

    if failures:
        sample = "; ".join(f"{row['stockId']}: {row['error']}" for row in failures[:8])
        raise RuntimeError(
            f"Yahoo history incomplete: {len(failures)}/{len(instruments)} instruments failed. {sample}"
        )

    today_count = sum(
        any(row.trade_date == today for row in histories[instrument.stock_id])
        for instrument in instruments
    )
    today_coverage = today_count / max(1, len(instruments))
    print(f"Today's coverage: {today_count}/{len(instruments)} = {today_coverage:.2%}", flush=True)
    if today_coverage < MIN_TODAY_COVERAGE:
        raise RuntimeError(
            f"Yahoo current-day coverage is too low ({today_coverage:.2%} < {MIN_TODAY_COVERAGE:.0%}); refusing to publish"
        )

    identities = [
        [row.stock_id, row.stock_name, row.market, row.instrument_type]
        for row in instruments
    ]
    months = sorted(
        {
            bar.trade_date.strftime("%Y-%m")
            for instrument in instruments
            for bar in histories[instrument.stock_id]
        }
    )
    month_index = {month: index for index, month in enumerate(months)}
    monthly_rows: list[list[Any]] = []
    features_by_id: dict[str, list[list[Any]]] = {}
    for stock_index, instrument in enumerate(instruments):
        features = monthly_features(histories[instrument.stock_id], today)
        features_by_id[instrument.stock_id] = features
        monthly_rows.append(
            [
                stock_index,
                [[month_index[item[0]], *item[1:]] for item in features],
            ]
        )

    benchmark_features = features_by_id.get("0050", [])
    benchmark_rows = [
        [month_index[item[0]], *item[1:]]
        for item in benchmark_features
        if item[0] in month_index
    ]
    generated_at = datetime.now(TAIPEI).isoformat(timespec="seconds")
    meta = {
        "startDate": START_DATE.isoformat(),
        "endDate": today.isoformat(),
        "stockCount": len(stocks),
        "etfCount": len(etfs),
        "instrumentCount": len(instruments),
        "requestedStockCount": len(stocks),
        "requestedEtfCount": len(etfs),
        "generatedAt": generated_at,
        "companySource": "公開資訊觀測站、證交所與櫃買中心",
        "priceSource": "Yahoo Finance",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    write_gzip_json(
        output_dir / "market_monthly.bin",
        {
            "meta": meta,
            "stocks": identities,
            "months": months,
            "rows": monthly_rows,
            "benchmark": benchmark_rows,
        },
    )

    years = list(range(START_DATE.year, today.year + 1))
    year_counts: dict[str, int] = {}
    for year in years:
        year_rows = {
            instrument.stock_id: [bar for bar in histories[instrument.stock_id] if bar.trade_date.year == year]
            for instrument in instruments
        }
        dates = sorted({bar.trade_date.isoformat() for rows in year_rows.values() for bar in rows})
        date_index = {trade_date: index for index, trade_date in enumerate(dates)}
        compact_rows: list[list[Any]] = []
        for instrument in instruments:
            values = [
                [date_index[bar.trade_date.isoformat()], bar.adj_close]
                for bar in year_rows[instrument.stock_id]
            ]
            if values:
                compact_rows.append([instrument.stock_id, values])
        year_counts[str(year)] = len(compact_rows)
        write_gzip_json(
            output_dir / f"market_daily_{year}.bin",
            {"year": year, "dates": dates, "rows": compact_rows},
        )

    fallback_ids = sorted(stock_id for stock_id, source in sources.items() if source != "yahoo-global")
    manifest = {
        "schemaVersion": 1,
        "startDate": START_DATE.isoformat(),
        "endDate": today.isoformat(),
        "generatedAt": generated_at,
        "monthlyFile": "market_monthly.bin",
        "years": years,
        "stockCount": len(stocks),
        "etfCount": len(etfs),
        "instrumentCount": len(instruments),
        "todayInstrumentCount": today_count,
        "todayCoveragePct": round(today_coverage * 100, 2),
        "yearInstrumentCounts": year_counts,
        "priceSource": "Yahoo Finance adjusted close",
        "fallbackInstrumentIds": fallback_ids,
    }
    (output_dir / "market_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "market_data_status.json").write_text(
        json.dumps(
            {
                "meta": meta,
                "availableStockCount": len(stocks),
                "availableEtfCount": len(etfs),
                "availableInstrumentCount": len(instruments),
                "todayInstrumentCount": today_count,
                "todayCoveragePct": round(today_coverage * 100, 2),
                "failedInstrumentCount": 0,
                "fallbackInstrumentIds": fallback_ids,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        f"READY: endDate={today.isoformat()} instruments={len(instruments)} "
        f"todayCoverage={today_coverage:.2%}",
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument(
        "--date",
        help="Override Taiwan date for a controlled/manual run (YYYY-MM-DD).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 1 <= args.workers <= 32:
        raise SystemExit("--workers must be between 1 and 32")
    today = date.fromisoformat(args.date) if args.date else datetime.now(TAIPEI).date()
    build_assets(args.output.resolve(), args.workers, today)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
