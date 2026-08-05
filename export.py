"""팀 전달용 내보내기 — 과거 백필 + 상시 수집을 합쳐 Parquet·CSV 로 굽고 구글드라이브에 올린다.

세 조각을 합친다:
  data/history/base=*/part-*.parquet   백필(2023-01-01~, 상폐 종목 포함)
  data/bars/date=*/part-*.parquet      상시 수집(가동 이후)
  → export/{parquet,csv}/              base 별 1파일

겹치는 구간이 있다(백필 끝 ≈ 수집 시작, 게다가 수집은 매 사이클 최근 5분을 다시 받는다).
(ts, base) 로 중복을 제거하되 **백필을 우선**한다 — 거래소가 확정한 봉이 실시간 스냅샷보다 정확하다.

CSV 는 gzip 으로 굽는다. 1분봉 10억 행을 날 CSV 로 두면 100GB 인데 gz 면 1/4 이고,
pandas·polars·R·duckdb 전부 .csv.gz 를 그대로 읽는다(엑셀만 압축을 못 푼다).
사람이 볼 `datetime` 열(UTC)을 덧붙인다 — ts(epoch ms)만 주면 분석가가 매번 변환해야 한다.
"""
from __future__ import annotations

import argparse
import csv
import logging
import subprocess
import time
from pathlib import Path

import duckdb

from collector import BARS, DATA

log = logging.getLogger("export")

HISTORY = DATA / "history"
EXPORT = DATA / "export"
COLS = "ts, venue, base, symbol, open, high, low, close, volume, quote_volume"

# 백필(src=0)이 실시간 스냅샷(src=1)을 이긴다.
UNION = """
  SELECT {cols}, 0 AS src FROM read_parquet('{hist}', union_by_name=true)
  UNION ALL
  SELECT {cols}, 1 AS src FROM read_parquet('{live}', hive_partitioning=1) WHERE base = '{base}'
"""
DEDUP = """
  SELECT {cols}, to_timestamp(ts/1000) AT TIME ZONE 'UTC' AS datetime FROM (
    SELECT *, row_number() OVER (PARTITION BY ts ORDER BY src) rn FROM ({union}))
  WHERE rn = 1 ORDER BY ts
"""


def bases() -> list[str]:
    return sorted(p.name.split("=", 1)[1] for p in HISTORY.glob("base=*") if p.is_dir())


def query(base: str) -> str:
    hist = HISTORY / f"base={base}" / "*.parquet"
    u = UNION.format(cols=COLS, hist=hist, live=f"{BARS}/**/*.parquet", base=base.replace("'", "''"))
    return DEDUP.format(cols=COLS, union=u)


def export_one(con, base: str, want_parquet: bool, want_csv: bool) -> int:
    q = query(base)
    n = 0
    if want_parquet:
        p = EXPORT / "parquet" / f"{base}.parquet"
        con.execute(f"COPY ({q}) TO '{p}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]
    if want_csv:
        p = EXPORT / "csv" / f"{base}.csv.gz"
        con.execute(f"COPY ({q}) TO '{p}' (FORMAT CSV, HEADER, COMPRESSION GZIP)")
        if not n:
            n = con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0]
    return n


def write_metadata(con, rows: list[dict]):
    """팀이 먼저 볼 요약 3종. 여기에 생존편향 처리 결과가 드러나야 한다."""
    d = EXPORT / "metadata"
    d.mkdir(parents=True, exist_ok=True)

    with open(d / "coverage.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, ["base", "source", "symbol", "rows", "first_utc", "last_utc",
                               "days", "delisted"])
        w.writeheader()
        w.writerows(rows)

    src = DATA / "history_universe.csv"
    if src.exists():
        (d / "universe.csv").write_bytes(src.read_bytes())

    live = sum(not r["delisted"] for r in rows)
    with open(d / "README.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["항목", "값"])
        for k, v in [
            ("해상도", "1분봉 (OHLCV)"),
            ("기간", "2023-01-01 ~ 현재"),
            ("종목 수", len(rows)),
            ("  현재 상장", live),
            ("  상장폐지(생존편향 보정)", len(rows) - live),
            ("총 행수", sum(r["rows"] for r in rows)),
            ("시각 기준", "ts = 봉 시작 UTC epoch ms / datetime = UTC"),
            ("컬럼", COLS.replace(" ", "") + ",datetime"),
            ("quote_volume", "달러 환산 거래대금 (거래소 미제공 시 빈값)"),
            ("출처", "binance 벌크덤프 · bybit kline REST · bybit 체결덤프 집계"),
        ]:
            w.writerow([k, v])
    log.info("메타데이터 3종 → %s", d)


def main(argv=None):
    p = argparse.ArgumentParser(description="팀 전달용 Parquet·CSV 내보내기 + 드라이브 업로드")
    # 기본은 Parquet 만(사장 지시 2026-08-05). CSV 는 필요할 때 --csv 로.
    p.add_argument("--csv", action="store_true")
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--no-parquet", dest="parquet", action="store_false")
    p.add_argument("--upload", action="store_true", help="rclone 으로 구글드라이브 전송")
    p.add_argument("--remote", default="gdrive:CryptoBars")
    p.add_argument("--limit", type=int, default=None, help="종목 수 제한(시험용)")
    a = p.parse_args(argv)
    if not (a.csv or a.parquet):
        a.parquet = True
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    for sub, want in (("parquet", a.parquet), ("csv", a.csv)):
        if want:                       # 안 만들 형식의 빈 폴더가 드라이브에 올라가지 않게
            (EXPORT / sub).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")

    upath = DATA / "history_universe.csv"
    uni = ({r["base"]: r for r in csv.DictReader(open(upath, newline="", encoding="utf-8"))}
           if upath.exists() else {})
    todo = bases()[:a.limit] if a.limit else bases()
    log.info("내보내기 %d종목 (parquet=%s csv=%s)", len(todo), a.parquet, a.csv)
    meta, t0 = [], time.time()
    for i, b in enumerate(todo, 1):
        try:
            n = export_one(con, b, a.parquet, a.csv)
            st = con.execute(f"""SELECT min(ts), max(ts), count(DISTINCT (ts/86400000)::BIGINT)
                                 FROM ({query(b)})""").fetchone()
            u = uni.get(b, {})
            meta.append({"base": b, "source": u.get("source", ""), "symbol": u.get("symbol", ""),
                         "rows": n,
                         "first_utc": time.strftime("%Y-%m-%d", time.gmtime((st[0] or 0) / 1000)),
                         "last_utc": time.strftime("%Y-%m-%d", time.gmtime((st[1] or 0) / 1000)),
                         "days": st[2] or 0,
                         # bybit 체결덤프에서만 나오는 종목 = kline API 가 안 주는 상폐 종목
                         "delisted": int(u.get("source") == "bybit_trades")})
        except Exception as e:
            log.warning("%s 실패: %s", b, str(e)[:150])
        if i % 100 == 0:
            log.info("%d/%d (%.0f%%) · %.0f분 경과", i, len(todo), i / len(todo) * 100,
                     (time.time() - t0) / 60)
    write_metadata(con, meta)
    log.info("굽기 완료: %d종목 %.1fM행 (%.1f분)",
             len(meta), sum(m["rows"] for m in meta) / 1e6, (time.time() - t0) / 60)

    if a.upload:
        log.info("드라이브 전송 → %s", a.remote)
        r = subprocess.run(["rclone", "copy", str(EXPORT), a.remote,
                            "--transfers", "8", "--checkers", "16", "--stats", "60s",
                            "--stats-one-line", "--progress"])
        log.info("전송 %s", "완료" if r.returncode == 0 else f"실패(rc={r.returncode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
