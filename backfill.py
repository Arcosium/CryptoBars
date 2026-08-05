"""과거 1분봉 대량 백필 — **생존편향 없는** 유니버스.

상시 수집기(`collector.py`)는 '지금부터'를 채운다. 이건 '그 전'을 채운다.

## 생존편향 (사장 지시 2026-08-05)

지금 상장돼 있는 종목만 모으면 **망한 코인이 전부 빠진다**. 살아남은 것만 보고
"코인 수익률이 이렇다"고 하면 그 숫자는 거짓이다. 그래서 대상은 '현재 상장'이 아니라
**기간 중 한 번이라도 거래된 전 종목**이다.

거래소별로 상폐 종목을 어디까지 주는지가 다르다:

| 소스 | 대상 | 상폐 종목 |
|---|---|---|
| `binance` 벌크 zip (data.binance.vision) | 913심볼 / 832base | ✅ 상폐돼도 파일이 남는다 |
| `bybit` kline REST | 현재 상장분만 | ❌ 상폐 심볼은 **빈 응답** |
| `bybit_trades` 체결 덤프 (public.bybit.com) | 상폐분 | ✅ 체결을 1분봉으로 집계 |

grvt·hyperliquid·paradex·backpack 은 이력 상한이 하루~며칠이라 과거를 못 준다(수집기가 앞으로 쌓는다).

## 저장

  data/history/base=<BASE>/part-<YYYY-MM>.parquet

상시 수집(`data/bars`, 일자 파티션)과 따로 둔다 — 백필은 종목별 월 단위가 자연스럽고
(한 달 43,200행이 파일 하나) 일자 파티션에 넣으면 파일이 수십만 개가 된다.
파일이 있으면 건너뛰므로 **몇 번이든 다시 돌려도 되고, 죽으면 이어서 받는다**.
"""
from __future__ import annotations

import argparse
import csv as csvmod
import gzip
import io
import logging
import re
import threading
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from collector import DATA, SCHEMA

log = logging.getLogger("backfill")

HISTORY = DATA / "history"
UNIVERSE_CSV = DATA / "history_universe.csv"
VISION = "https://data.binance.vision/data/futures/um"
VISION_S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BYBIT_KLINE = "https://api.bybit.com/v5/market/kline"
BYBIT_TRADES = "https://public.bybit.com/trading"

QUOTES = ("USDT", "USDC", "BUSD", "USD")      # 긴 것 먼저 — 'ADABUSD' 를 'ADAB'+USD 로 자르면 안 된다

# 한 코인이 여러 마켓에 상장돼 있을 때 어느 마켓을 쓸지. **알파벳 순으로 고르면 안 된다** —
# 'BTCBUSD' < 'BTCUSDT' 라서 BUSD 가 이기는데, binance 가 BUSD 를 2023~24 에 폐지해
# BTC·ETH·BNB 같은 메이저가 2024년 중반부터 통째로 비어버린다(2026-08-05 실측·수정).
QUOTE_RANK = {"USDT": 0, "USDC": 1, "USD": 2, "BUSD": 3}


def quote_rank(symbol: str) -> int:
    for q in QUOTES:
        if symbol.endswith(q):
            return QUOTE_RANK.get(q, 9)
    return 9


def base_of(symbol: str) -> str:
    for q in QUOTES:
        if symbol.endswith(q):
            return symbol[:-len(q)]
    return symbol


def months(start: datetime, end: datetime) -> list[str]:
    y, m, out = start.year, start.month, []
    while (y, m) <= (end.year, end.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def month_bounds(ym: str) -> tuple[int, int]:
    y, m = (int(x) for x in ym.split("-"))
    a = datetime(y, m, 1, tzinfo=timezone.utc)
    b = datetime(y + (m == 12), (m % 12) + 1, 1, tzinfo=timezone.utc)
    return int(a.timestamp() * 1000), int(b.timestamp() * 1000)


def out_path(base: str, ym: str) -> Path:
    return HISTORY / f"base={base}" / f"part-{ym}.parquet"


def write(base: str, ym: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    rows.sort(key=lambda r: r["ts"])
    p = out_path(base, ym)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    pq.write_table(pa.table({f.name: [r[f.name] for r in rows] for f in SCHEMA}, schema=SCHEMA),
                   tmp, compression="zstd")
    tmp.replace(p)                 # 원자적 — 중간에 죽어도 반쪽 파일이 안 남는다
    return len(rows)


# ── binance: 공식 벌크 zip (상폐 종목도 파일이 남아 있다) ────────────────
def _vision_rows(c, url, base, symbol, lo, hi):
    r = c.get(url)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    out = []
    with zipfile.ZipFile(io.BytesIO(r.content)) as z, z.open(z.namelist()[0]) as fh:
        for row in csvmod.reader(io.TextIOWrapper(fh, "utf-8")):
            if not row or not row[0].isdigit():
                continue                                   # 헤더 줄(신규 파일에만 있다)
            ts = int(row[0])
            if ts > 10 ** 14:                              # 일부 파일은 open_time 이 마이크로초다
                ts //= 1000
            if lo <= ts < hi:
                out.append({"ts": ts, "venue": "binance", "base": base, "symbol": symbol,
                            "open": float(row[1]), "high": float(row[2]), "low": float(row[3]),
                            "close": float(row[4]), "volume": float(row[5]),
                            "quote_volume": float(row[7])})
    return out


def bf_binance(c, base, symbol, ym) -> int:
    lo, hi = month_bounds(ym)
    rows = _vision_rows(c, f"{VISION}/monthly/klines/{symbol}/1m/{symbol}-1m-{ym}.zip", base, symbol, lo, hi)
    if not rows:
        # 이번 달은 월별 zip 이 아직 없다 → 일별 zip 으로 긁는다.
        d = datetime(*(int(x) for x in ym.split("-")), 1, tzinfo=timezone.utc)
        today = datetime.now(timezone.utc).date()
        while d.date() < today and d.strftime("%Y-%m") == ym:
            rows += _vision_rows(c, f"{VISION}/daily/klines/{symbol}/1m/{symbol}-1m-{d:%Y-%m-%d}.zip",
                                 base, symbol, lo, hi)
            d += timedelta(days=1)
    return write(base, ym, rows)


# ── bybit: kline REST 페이징 (현재 상장분만 응답한다) ────────────────────
def bf_bybit(c, base, symbol, ym) -> int:
    lo, hi = month_bounds(ym)
    rows, end = [], hi - 1
    while end >= lo:
        lst = ((c.get(BYBIT_KLINE, params={"category": "linear", "symbol": symbol, "interval": "1",
                                           "start": lo, "end": end, "limit": 1000})
                .json().get("result") or {}).get("list") or [])
        if not lst:
            break
        for k in lst:                                       # 내림차순으로 온다
            ts = int(k[0])
            if lo <= ts < hi:
                rows.append({"ts": ts, "venue": "bybit", "base": base, "symbol": symbol,
                             "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                             "close": float(k[4]), "volume": float(k[5]),
                             "quote_volume": float(k[6])})
        oldest = min(int(k[0]) for k in lst)
        if oldest <= lo:
            break
        end = oldest - 1
    return write(base, ym, rows)


# ── bybit 상폐: 체결 덤프를 1분봉으로 집계 ───────────────────────────────
_files_cache: dict[str, list[str]] = {}
_files_lock = threading.Lock()


def _bybit_trade_files(c, symbol) -> list[str]:
    """심볼 디렉터리를 한 번만 읽어 파일명을 캐시. 날짜를 하나씩 찔러보면 요청이 수십만 건이 된다."""
    with _files_lock:
        if symbol in _files_cache:
            return _files_cache[symbol]
    names = re.findall(rf'({re.escape(symbol)}\d{{4}}-\d{{2}}-\d{{2}}\.csv\.gz)',
                       c.get(f"{BYBIT_TRADES}/{symbol}/").text)
    names = sorted(set(names))
    with _files_lock:
        _files_cache[symbol] = names
    return names


def bf_bybit_trades(c, base, symbol, ym) -> int:
    day_files = [n for n in _bybit_trade_files(c, symbol) if n[len(symbol):len(symbol) + 7] == ym]
    if not day_files:
        return 0
    bars: dict[int, dict] = {}
    for name in day_files:
        r = c.get(f"{BYBIT_TRADES}/{symbol}/{name}")
        if r.status_code != 200:
            continue
        with gzip.open(io.BytesIO(r.content), "rt") as fh:
            rd = csvmod.DictReader(fh)
            for t in rd:
                try:
                    ts = int(float(t["timestamp"]) * 1000) // 60_000 * 60_000
                    px, sz = float(t["price"]), float(t["size"])
                except (KeyError, TypeError, ValueError):
                    continue
                qv = float(t.get("foreignNotional") or 0) or px * sz
                b = bars.get(ts)
                if b is None:
                    bars[ts] = {"ts": ts, "venue": "bybit", "base": base, "symbol": symbol,
                                "open": px, "high": px, "low": px, "close": px,
                                "volume": sz, "quote_volume": qv}
                else:
                    b["high"] = max(b["high"], px)
                    b["low"] = min(b["low"], px)
                    b["close"] = px          # 파일이 시간순이라 마지막 체결이 종가가 된다
                    b["volume"] += sz
                    b["quote_volume"] += qv
    return write(base, ym, list(bars.values()))


SOURCES = {"binance": bf_binance, "bybit": bf_bybit, "bybit_trades": bf_bybit_trades}


# ── 유니버스: 기간 중 한 번이라도 거래된 전 종목 ─────────────────────────
def _binance_ever(c) -> list[str]:
    """벌크 버킷을 훑어 '데이터가 존재하는' 전 심볼. 상폐분이 여기 남아 있다."""
    out, marker = [], ""
    while True:
        t = c.get(f"{VISION_S3}?delimiter=/&prefix=data/futures/um/monthly/klines/"
                  + (f"&marker={marker}" if marker else "")).text
        got = re.findall(r"<Prefix>data/futures/um/monthly/klines/([^/]+)/</Prefix>", t)
        out += got
        if "<IsTruncated>true</IsTruncated>" not in t or not got:
            return [s for s in out if s.endswith(QUOTES)]
        marker = f"data/futures/um/monthly/klines/{got[-1]}/"


def _bybit_ever(c) -> tuple[list[str], list[str]]:
    """(현재 상장, 상폐) 심볼. 상폐는 체결 덤프에만 남아 있다."""
    ever = sorted(set(re.findall(r'href="([A-Z0-9]+)/"', c.get(f"{BYBIT_TRADES}/").text)))
    ever = [s for s in ever if s.endswith(QUOTES)]
    live, cursor = set(), ""
    while True:
        p = {"category": "linear", "limit": 1000}
        if cursor:
            p["cursor"] = cursor
        res = c.get("https://api.bybit.com/v5/market/instruments-info", params=p).json()["result"]
        live |= {s["symbol"] for s in res["list"]}
        cursor = res.get("nextPageCursor") or ""
        if not cursor:
            break
    return [s for s in ever if s in live], [s for s in ever if s not in live]


def history_universe(c, rebuild=False) -> list[dict]:
    """base 1개당 소스 1개. binance 벌크가 제일 싸고 상폐도 주므로 최우선.

    binance 에 없는 base 만 bybit 로 넘긴다 — 상장 중이면 kline REST, 상폐면 체결 덤프.
    """
    if UNIVERSE_CSV.exists() and not rebuild:
        with open(UNIVERSE_CSV, newline="", encoding="utf-8") as f:
            return list(csvmod.DictReader(f))

    bn = _binance_ever(c)
    by_live, by_gone = _bybit_ever(c)
    seen, out = set(), []

    def add(symbol, source):
        b = base_of(symbol)
        if b and b not in seen:
            seen.add(b)
            out.append({"base": b, "symbol": symbol, "source": source})

    key = lambda s: (base_of(s), quote_rank(s), s)   # noqa: E731 — 마켓 우선순위대로 먼저 오게
    for s in sorted(bn, key=key):
        add(s, "binance")
    for s in sorted(by_live, key=key):
        add(s, "bybit")
    for s in sorted(by_gone, key=key):
        add(s, "bybit_trades")

    UNIVERSE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(UNIVERSE_CSV, "w", newline="", encoding="utf-8") as f:
        w = csvmod.DictWriter(f, ["base", "symbol", "source"])
        w.writeheader()
        w.writerows(out)
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description="과거 1분봉 백필 (생존편향 없는 전 종목)")
    p.add_argument("--start", default="2023-01-01", help="시작일 YYYY-MM-DD")
    p.add_argument("--workers", type=int, default=12)
    p.add_argument("--source", default=None, help="binance / bybit / bybit_trades 만")
    p.add_argument("--limit", type=int, default=None, help="종목 수 제한(시험용)")
    p.add_argument("--rebuild-universe", action="store_true")
    p.add_argument("--plan", action="store_true", help="대상만 세고 종료")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    c = httpx.Client(timeout=180.0, follow_redirects=True,
                     headers={"User-Agent": "CryptoBars/1.0"},
                     limits=httpx.Limits(max_connections=a.workers * 2))
    end = datetime.now(timezone.utc)
    start = datetime.strptime(a.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    yms = months(start, end)

    uni = history_universe(c, rebuild=a.rebuild_universe)
    if a.source:
        uni = [u for u in uni if u["source"] == a.source]
    if a.limit:
        uni = uni[:a.limit]
    per = defaultdict(int)
    for u in uni:
        per[u["source"]] += 1

    jobs = [(u, ym) for u in uni for ym in yms if not out_path(u["base"], ym).exists()]
    log.info("%s~ %d개월 · %d종목 %s → %d작업 (이미 받음 %d)",
             a.start, len(yms), len(uni), dict(per), len(jobs), len(uni) * len(yms) - len(jobs))
    if a.plan:
        return 0

    done = rows = fail = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        futs = {ex.submit(SOURCES[u["source"]], c, u["base"], u["symbol"], ym): (u, ym)
                for u, ym in jobs}
        for fut in as_completed(futs):
            u, ym = futs[fut]
            done += 1
            try:
                rows += fut.result()
            except Exception as e:
                fail += 1
                log.warning("%s %s %s 실패: %s", u["source"], u["base"], ym, str(e)[:120])
            if done % 500 == 0:
                el = time.time() - t0
                log.info("%d/%d (%.1f%%) %.1fM행 실패%d · %.0f작업/분 · 남은 %.0f분",
                         done, len(jobs), done / len(jobs) * 100, rows / 1e6, fail,
                         done / el * 60, (len(jobs) - done) / (done / el) / 60)
    log.info("백필 완료: %d작업 %.1fM행 실패%d (%.1f분)", done, rows / 1e6, fail, (time.time() - t0) / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
