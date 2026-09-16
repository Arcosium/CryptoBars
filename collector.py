"""크립토 전 종목 1분봉 상시 수집기 — 백그라운드 데몬.

설계는 QuantInSight `market_bars/crawler.py`(KRX 분봉)와 같은 원칙이다:
  · 매분 고정 시각에 증분 수집, 매번 최근 몇 분을 **다시** 받아 지연확정·재시작 공백을 스스로 메운다
  · 어떤 예외도 루프를 죽이지 않는다(fail-safe)
  · 지나간 분봉은 다시 살 수 없으므로 **영구 보존**(자동 삭제 없음)

KRX 와 다른 점은 두 가지뿐이다:
  · 24/7 이라 장 시간 판정이 없다
  · 종목이 776개(=KRX 350개의 2배)에 24시간이라 SQLite 대신 **일자별 Parquet** 에 쌓는다
    (하루 약 112만 행 ≈ 25MB. SQLite 단일 테이블로는 1년이면 4억 행이 된다)

종목 1개당 거래소 1곳만 본다 — 24h 거래대금이 가장 큰 곳(`build_universe`). 같은 코인을
6개 거래소에서 중복 수집하면 용량만 6배가 되고 가격은 사실상 같다. 거래소간 스프레드가
필요하면 그건 MultiEX 의 capture lane 이 하는 일이다.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import venues

log = logging.getLogger("cryptobars")

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv("CRYPTOBARS_DATA", ROOT / "data"))
BARS = DATA / "bars"
UNIVERSE_CSV = DATA / "universe.csv"

# 매분 이 초에 수집(거래소가 직전 봉을 확정할 여유). KRX 크롤러의 02~05초와 같은 취지.
CYCLE_SECOND = int(os.getenv("CRYPTOBARS_CYCLE_SECOND", "8"))
# 매 사이클 되받는 봉 수. 1개만 받으면 한 번 실패한 분이 영영 빈다.
LOOKBACK = int(os.getenv("CRYPTOBARS_LOOKBACK", "5"))
# 기동 직후 1회. 재시작 공백을 메운다(4시간).
BACKFILL = int(os.getenv("CRYPTOBARS_BACKFILL", "240"))
FLUSH_MINUTES = int(os.getenv("CRYPTOBARS_FLUSH_MINUTES", "10"))
UNIVERSE_TTL_H = int(os.getenv("CRYPTOBARS_UNIVERSE_TTL_H", "6"))
# 하루 한 번 지난 날짜 버퍼를 history(정본)로 접는다. 백필이 도는 동안엔 0 으로 꺼둔다.
COMPACT = os.getenv("CRYPTOBARS_COMPACT", "1") not in ("0", "false", "")
ENABLED = tuple(v for v in os.getenv("CRYPTOBARS_VENUES", ",".join(venues.VENUES)).split(",") if v)

SCHEMA = pa.schema([
    ("ts", pa.int64()), ("venue", pa.string()), ("base", pa.string()), ("symbol", pa.string()),
    ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("volume", pa.float64()), ("quote_volume", pa.float64()),
])


def now_ms() -> int:
    return int(time.time() * 1000)


# ── 유니버스 ────────────────────────────────────────────────────────────
def build_universe(client) -> list[dict]:
    """거래소 6곳의 상장 목록 → base 별 '24h 거래대금 1위' 거래소 1곳으로 확정.

    코인 quote(ETHBTC 등)는 제외한다 — 거래대금 단위가 달러가 아니라 비교가 안 된다.
    한 거래소가 통째로 실패해도 나머지로 유니버스를 만든다(전부 실패할 때만 예외).
    """
    best: dict[str, dict] = {}
    errors = []
    for venue in ENABLED:
        try:
            for r in venues.LISTERS[venue](client):
                if r["quote"].upper() not in venues.USD_QUOTES or not r["base"]:
                    continue
                b = r["base"].upper()
                if b not in best or r["volume_24h_usd"] > best[b]["volume_24h_usd"]:
                    best[b] = {"base": b, "venue": venue, "symbol": r["symbol"],
                               "volume_24h_usd": r["volume_24h_usd"]}
        except Exception as e:
            errors.append(f"{venue}: {type(e).__name__}: {e}")
            log.warning("유니버스 %s 조회 실패: %s", venue, e)
    if not best:
        raise RuntimeError(f"유니버스가 비었다 — 전 거래소 실패: {errors}")

    rows = sorted(best.values(), key=lambda r: -r["volume_24h_usd"])
    UNIVERSE_CSV.parent.mkdir(parents=True, exist_ok=True)
    tmp = UNIVERSE_CSV.with_suffix(".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, ["base", "venue", "symbol", "volume_24h_usd"])
        w.writeheader()
        w.writerows(rows)
    tmp.replace(UNIVERSE_CSV)      # 원자적 교체 — 읽는 쪽이 반쪽 파일을 보지 않게
    log.info("유니버스 %d종목 갱신 (실패 거래소 %d)", len(rows), len(errors))
    return rows


def load_universe(client, max_age_h=UNIVERSE_TTL_H) -> list[dict]:
    """CSV 가 신선하면 그대로, 아니면 새로 만든다. 갱신이 실패하면 묵은 CSV 로 계속 간다."""
    fresh = (UNIVERSE_CSV.exists()
             and time.time() - UNIVERSE_CSV.stat().st_mtime < max_age_h * 3600)
    if not fresh:
        try:
            return build_universe(client)
        except Exception as e:
            log.error("유니버스 갱신 실패(%s) — 기존 CSV 로 계속", e)
            if not UNIVERSE_CSV.exists():
                raise
    with open(UNIVERSE_CSV, newline="", encoding="utf-8") as f:
        return [{**r, "volume_24h_usd": float(r["volume_24h_usd"])} for r in csv.DictReader(f)]


# ── 수집 ────────────────────────────────────────────────────────────────
def fetch_cycle(client, universe, limit, cutoff_ms) -> tuple[list[dict], list[str]]:
    """유니버스 전체의 최근 `limit`개 1분봉 수집. 아직 안 닫힌 봉은 버린다.

    거래소별로 워커 수를 따로 둔다 — 한 풀에 몰아 돌리면 요청이 적은 거래소가
    많은 거래소의 rate limit 에 같이 묶여 IP 차단을 부른다.
    """
    by_venue: dict[str, list[dict]] = {}
    for u in universe:
        by_venue.setdefault(u["venue"], []).append(u)

    rows, errs = [], []

    def one(u):
        return venues.KLINES[u["venue"]](client, u["symbol"], u["base"], limit, cutoff_ms)

    for venue, items in by_venue.items():
        with ThreadPoolExecutor(max_workers=venues.WORKERS.get(venue, 4)) as ex:
            futs = {ex.submit(one, u): u for u in items}
            for fut in as_completed(futs):
                u = futs[fut]
                try:
                    rows += fut.result()
                except Exception as e:
                    errs.append(f"{u['venue']}:{u['symbol']}: {type(e).__name__}: {e}")
    closed = [r for r in rows if r["ts"] + 60_000 <= cutoff_ms]
    return closed, errs


class Store:
    """(ts, base) 로 중복 제거한 버퍼를 일자별 Parquet part 로 flush.

    part 파일이 여러 개 생기고 재시작 시 겹치는 구간을 다시 받으므로 **파일 간 중복은 남는다**.
    읽을 때 `read_bars()` 가 (ts, base) 최신 1건만 남기므로 그대로 둔다 — 중복 제거하자고
    compaction 데몬을 하나 더 돌리는 것보다 싸다.
    """

    def __init__(self, root: Path = BARS):
        self.root = Path(root)
        self.buf: dict[tuple[int, str], dict] = {}

    def add(self, rows):
        for r in rows:
            self.buf[(r["ts"], r["base"])] = r

    def flush(self) -> int:
        if not self.buf:
            return 0
        by_date: dict[str, list[dict]] = {}
        for r in self.buf.values():
            d = datetime.fromtimestamp(r["ts"] / 1000, timezone.utc).strftime("%Y-%m-%d")
            by_date.setdefault(d, []).append(r)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        n = 0
        for d, rows in by_date.items():
            out = self.root / f"date={d}"
            out.mkdir(parents=True, exist_ok=True)
            cols = {f.name: [r[f.name] for r in rows] for f in SCHEMA}
            pq.write_table(pa.table(cols, schema=SCHEMA), out / f"part-{stamp}.parquet",
                           compression="zstd")
            n += len(rows)
        self.buf.clear()
        return n


def _compact_safely():
    """수집 루프가 compaction 예외로 죽지 않게 감싼다."""
    try:
        from compact import compact_once
        log.info("compaction 결과: %s", compact_once())
    except Exception as e:
        log.error("compaction 실패(수집은 계속): %s", e)


def run_forever(stop=None, cycles=None):
    """메인 루프. 매분 CYCLE_SECOND 초에 수집하고, 어떤 예외도 루프를 죽이지 않는다."""
    BARS.mkdir(parents=True, exist_ok=True)
    client = venues.client()
    universe = load_universe(client)
    store = Store()
    log.info("수집 시작: %d종목 / 거래소 %s", len(universe),
             sorted({u["venue"] for u in universe}))

    limit, done, last_flush = max(BACKFILL, LOOKBACK), 0, time.time()
    last_compact_day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    while not (stop and stop.is_set()) and (cycles is None or done < cycles):
        # 다음 분 CYCLE_SECOND 초까지 대기
        nxt = (int(time.time()) // 60 + 1) * 60 + CYCLE_SECOND
        while time.time() < nxt:
            if stop and stop.wait(min(1.0, nxt - time.time())):
                break
            time.sleep(min(1.0, max(0.0, nxt - time.time())))
        if stop and stop.is_set():
            break
        try:
            rows, errs = fetch_cycle(client, universe, limit, now_ms())
            store.add(rows)
            log.info("cycle: %d행 / 실패 %d종목%s", len(rows), len(errs),
                     f" (예: {errs[0]})" if errs else "")
            if errs and len(errs) == len(universe):
                log.error("전 종목 수집 실패 — 네트워크/차단 점검 필요")
        except Exception as e:                       # fail-safe
            log.error("cycle 예외: %s", e)
        limit = LOOKBACK                             # 백필은 1회만
        done += 1

        if time.time() - last_flush >= FLUSH_MINUTES * 60:
            try:
                log.info("flush: %d행 저장", store.flush())
            except Exception as e:
                log.error("flush 실패(버퍼 유지): %s", e)
            last_flush = time.time()
            try:
                universe = load_universe(client)     # TTL 지나면 내부에서 갱신
            except Exception as e:
                log.error("유니버스 재적재 실패: %s", e)
            # UTC 날짜가 바뀌면 지난 날짜 버퍼를 history(정본)로 접는다. 수집 루프를 막지 않게
            # 별도 스레드로 돌린다 — 한 달 파일을 통째로 다시 쓰는 작업이라 몇 분 걸릴 수 있다.
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if COMPACT and today != last_compact_day:
                last_compact_day = today
                threading.Thread(target=_compact_safely, name="compact", daemon=True).start()
    log.info("종료 flush: %d행 저장", store.flush())


# ── 읽기 ────────────────────────────────────────────────────────────────
def read_bars(where=""):
    """과거(history) + 아직 안 접힌 수집분(bars)을 합쳐 (ts, base) 중복을 제거한 DataFrame.

    예: read_bars("base = 'BTC'"). 두 저장소를 다 봐야 하는 이유는 compaction 이 하루 한 번이라
    오늘 치는 아직 bars 에만 있기 때문이다. 겹치면 history 를 우선한다(거래소 확정봉).
    """
    import duckdb
    cols = ", ".join(f.name for f in SCHEMA)
    parts = []
    if any((DATA / "history").glob("base=*")):
        parts.append(f"SELECT {cols}, 0 src FROM read_parquet('{DATA}/history/**/*.parquet',"
                     " union_by_name=true)")
    if any(BARS.glob("date=*")):
        parts.append(f"SELECT {cols}, 1 src FROM read_parquet('{BARS}/**/*.parquet',"
                     " hive_partitioning=1)")
    if not parts:
        import pandas as pd
        return pd.DataFrame(columns=[f.name for f in SCHEMA])
    union = " UNION ALL ".join(parts)
    cond = f"WHERE {where}" if where else ""
    return duckdb.sql(f"""
        SELECT {cols} FROM (
          SELECT *, row_number() OVER (PARTITION BY ts, base ORDER BY src) rn
          FROM ({union}) {cond})
        WHERE rn = 1 ORDER BY ts, base""").df()


def main(argv=None):
    p = argparse.ArgumentParser(description="크립토 전 종목 1분봉 수집기")
    p.add_argument("--once", action="store_true", help="1사이클만 돌고 종료(검증용)")
    p.add_argument("--cycles", type=int, default=None, help="N사이클 후 종료")
    p.add_argument("--refresh-universe", action="store_true", help="유니버스만 갱신하고 종료")
    p.add_argument("--stats", action="store_true", help="쌓인 데이터 요약 출력")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)   # 요청 1건당 1줄이면 로그가 분당 776줄

    if a.refresh_universe:
        print(f"{len(build_universe(venues.client()))} 종목 → {UNIVERSE_CSV}")
        return 0
    if a.stats:
        import duckdb
        # `rows` 는 DuckDB 예약어라 별칭으로 못 쓴다.
        # n_rows 는 part 간 중복을 포함한 날것, n_unique 가 read_bars() 가 실제로 돌려주는 수다.
        print(duckdb.sql(f"""SELECT date, count(*) n_rows,
                                    count(DISTINCT ts || '|' || base) n_unique,
                                    count(DISTINCT base) bases, count(DISTINCT ts) n_minutes
                             FROM read_parquet('{BARS}/**/*.parquet', hive_partitioning=1)
                             GROUP BY 1 ORDER BY 1""").df().to_string())
        return 0
    # systemd stop/restart 때 버퍼를 흘려보내고 끝낸다(안 그러면 최대 FLUSH_MINUTES 분이 날아간다).
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    def supervise_equities():
        while not stop.is_set():
            child = subprocess.Popen([
                os.getenv("EQUITYBARS_PYTHON", "/usr/bin/python3.12"),
                str(ROOT / "equities.py")])
            while child.poll() is None and not stop.wait(2):
                pass
            if stop.is_set() and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            if not stop.is_set():
                log.warning("Equity collector exited; restarting in 10 seconds")
                stop.wait(10)
    companion = None
    if not a.once and os.getenv("EQUITYBARS_ENABLED", "1") == "1":
        companion = threading.Thread(target=supervise_equities, name="equity-bars", daemon=True)
        companion.start()
    try:
        run_forever(stop=stop, cycles=1 if a.once else a.cycles)
    finally:
        stop.set()
        if companion:
            companion.join(timeout=45)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
