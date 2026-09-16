"""KRX legacy archive + venue-specific NXT bars under CryptoBars/data.

KIS is used exclusively for quotations. No broker order method is called.
The existing KRX crawler stays intact and has one writer, owned by CryptoBars.
"""
from __future__ import annotations

import asyncio
import csv
import fcntl
import json
import logging
import math
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
DATA = Path(os.getenv("CRYPTOBARS_DATA", ROOT / "data"))
QIS = ROOT.parent / "QuantInSight"
KST = ZoneInfo("Asia/Seoul")
log = logging.getLogger("marketbars.equities")


def open_store(path=None):
    p = Path(path or DATA / "NXT" / "bars.db")
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(p, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("""CREATE TABLE IF NOT EXISTS bars (
        code TEXT NOT NULL, ts TEXT NOT NULL,
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
        close REAL NOT NULL, volume REAL NOT NULL, source TEXT NOT NULL,
        PRIMARY KEY(code,ts))""")
    c.execute("CREATE INDEX IF NOT EXISTS bars_ts ON bars(ts)")
    return c


def parse_nxt(rows, now=None):
    now = now or datetime.now(KST)
    cutoff = now.replace(second=0, microsecond=0)
    out = []
    for r in rows:
        try:
            stamp = datetime.strptime(r["stck_bsop_date"] + r["stck_cntg_hour"], "%Y%m%d%H%M%S").replace(tzinfo=KST)
            # Ignore unfinished bars and any unexpected previous-day payload.
            if stamp >= cutoff or stamp.date() != now.date():
                continue
            o,h,l,c,v = [float(r[k]) for k in ("stck_oprc", "stck_hgpr", "stck_lwpr", "stck_prpr", "cntg_vol")]
            if not all(math.isfinite(x) for x in (o,h,l,c,v)) or min(o,h,l,c) <= 0 or v < 0:
                continue
            if h < max(o,c,l) or l > min(o,c,h):
                continue
            out.append((stamp.strftime("%Y%m%d%H%M"), o,h,l,c,v,"KIS:NX"))
        except (ValueError, TypeError, KeyError):
            continue
    return sorted(out)


def store_nxt(conn, code, rows):
    with conn:
        conn.executemany("INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?)",
                         [(code, *r) for r in rows])


async def collect_nxt_symbol(broker, code, latest, stop, now=None):
    """Page backwards to the last stored bar or today's first print.

    Re-read the overlapping boundary for vendor corrections. The API provides
    only intraday recovery, so prior-day gaps remain visible rather than filled.
    """
    now = now or datetime.now(KST)
    cursor = now.strftime("%H%M%S")
    collected = {}
    for _ in range(32):
        if stop.is_set():
            break
        d = await broker._get_json(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "FHKST03010200", {"FID_COND_MRKT_DIV_CODE": "NX", "FID_INPUT_ISCD": code,
            "FID_INPUT_HOUR_1": cursor, "FID_PW_DATA_INCU_YN": "Y", "FID_ETC_CLS_CODE": ""})
        if d.get("rt_cd") != "0":
            raise RuntimeError("NXT quotation rejected")
        rows = parse_nxt(d.get("output2") or [], now=now)
        if not rows:
            break
        for row in rows:
            collected[row[0]] = row
        oldest = rows[0][0]
        if (latest and oldest <= latest) or oldest[-4:] <= "0800":
            break
        stamp = datetime.strptime(oldest, "%Y%m%d%H%M") - timedelta(seconds=1)
        next_cursor = stamp.strftime("%H%M%S")
        if next_cursor >= cursor:
            break  # provider ignored the cursor; never loop forever
        cursor = next_cursor
    return [collected[k] for k in sorted(collected)]


def nxt_session(now):
    # Include a short post-close grace period to receive the final print.
    return now.weekday() < 5 and 800 <= int(now.strftime("%H%M")) <= 2005


def read_broker():
    sys.path.insert(0, str(QIS))
    from infra import auth_store, user_paths
    from infra.kis_broker import KISBroker
    with auth_store._connect() as conn:
        ids = [r[0] for r in conn.execute("SELECT id FROM users WHERE is_admin=1")]
    for uid in ids:
        for p in auth_store.list_profiles(uid):
            if p["kind"] == auth_store.PROFILE_KIS_REAL:
                creds = auth_store.get_user_credentials(p["uid"])
                return KISBroker(creds, token_path=user_paths.token_path(p["uid"]))
    raise RuntimeError("NXT quotation credentials unavailable")


def save_status(status):
    p = DATA / "equities_status.json"
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(status, ensure_ascii=False))
    tmp.replace(p)


async def run(stop):
    sys.path.insert(0, str(QIS))
    os.environ["LEADLAG_BARS_DB"] = str(DATA / "KRX" / "bars.db")
    os.environ["TIMEFOLIO_UNIVERSE_CSV"] = str(DATA / "KRX" / "universe.csv")
    from market_bars import crawler, market_time
    import requests
    codes = [c for c,_ in crawler.load_universe()]
    kr = crawler.Crawler(codes)
    nxt = open_store()
    broker = None
    status = {"KRX": {}, "NXT": {}}
    def kr_cycle():
        conn = crawler.open_db()
        try:
            with requests.Session() as session:
                return kr.crawl_cycle(conn, session)
        finally:
            conn.close()
    async def nxt_cycle(now):
        nonlocal broker
        if broker is None:
            broker = read_broker()
        count = empty = rows_count = 0
        for code in codes:
            if stop.is_set():
                break
            try:
                latest = nxt.execute("SELECT MAX(ts) FROM bars WHERE code=?", (code,)).fetchone()[0]
                rows = await collect_nxt_symbol(broker, code, latest, stop)
                store_nxt(nxt, code, rows)
                count += bool(rows); rows_count += len(rows)
            except Exception as exc:
                empty += 1
                log.warning("NXT %s: %s", code, type(exc).__name__)
        return {"symbols": count, "rows": rows_count, "errors": empty,
                "updated_at": datetime.now(KST).isoformat(), "source": "KIS:NX"}
    try:
        while not stop.is_set():
            now = datetime.now(KST)
            async def collect_kr():
                if market_time.in_crawl_session(now):
                    try:
                        n = await asyncio.to_thread(kr_cycle)
                        status["KRX"] = {"symbols": n, "updated_at": datetime.now(KST).isoformat(), "source": "Naver:siseJson"}
                    except Exception as exc:
                        status["KRX"]["error"] = type(exc).__name__
            async def collect_nxt():
                if nxt_session(now):
                    try:
                        status["NXT"] = await nxt_cycle(now)
                    except Exception as exc:
                        status["NXT"]["error"] = type(exc).__name__
            await asyncio.gather(collect_kr(), collect_nxt())
            status["heartbeat"] = datetime.now(KST).isoformat()
            save_status(status)
            log.info("KRX symbols=%s NXT symbols=%s", status["KRX"].get("symbols",0), status["NXT"].get("symbols",0))
            delay = max(1, 60 - time.time() % 60 + 5)
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except asyncio.TimeoutError:
                pass
    finally:
        nxt.close()
        if broker:
            await broker.close()


async def main():
    DATA.mkdir(parents=True, exist_ok=True)
    with (DATA / ".equities.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        await run(stop)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
