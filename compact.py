"""상시 수집분을 과거 데이터에 얹어 한 덩어리로 유지한다.

백필과 수집기는 쓰는 모양이 다르다. 백필은 종목별 월 단위(한 파일에 43,200행)가 자연스럽고,
수집기는 매 10분 flush 라 일자별로 쌓을 수밖에 없다. 그대로 두면 저장소가 둘로 갈린다.

이 모듈이 매일 한 번 그 둘을 합친다:

  data/bars/date=YYYY-MM-DD/*.parquet   (수집기가 쓰는 버퍼)
        ↓  하루가 끝나면
  data/history/base=<BASE>/part-<YYYY-MM>.parquet   (정본 — 3년치가 여기 있다)

**지난 UTC 날짜만** 접는다. 오늘 치는 수집기가 아직 쓰는 중이라 건드리지 않는다.
접고 나면 그 날짜 폴더는 통째로 지운다 — 같은 데이터를 두 군데 두면 언젠가 어긋난다.

Parquet 은 이어붙일 수 없어서 해당 월 파일을 통째로 다시 쓴다. 한 달이 최대 4만여 행이라
다시 쓰는 비용이 싸고, 임시 파일에 쓴 뒤 원자적으로 바꾸므로 중간에 죽어도 반쪽이 안 남는다.

⚠ 백필이 도는 중에는 절대 돌리지 마라. 같은 월 파일을 양쪽에서 쓰면 한쪽이 사라진다.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector import BARS, DATA, SCHEMA

log = logging.getLogger("cryptobars.compact")

HISTORY = DATA / "history"


def backfill_running() -> bool:
    return subprocess.run(["pgrep", "-f", "backfill.py --start"],
                          capture_output=True).returncode == 0


def _closed_date_dirs() -> list[Path]:
    """오늘(UTC)보다 이전 날짜 폴더만. 오늘 치는 수집기가 아직 채우는 중이다."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return sorted(p for p in BARS.glob("date=*")
                  if p.is_dir() and p.name.split("=", 1)[1] < today)


def _merge_month(base: str, ym: str, new_rows: list[dict]) -> int:
    """기존 월 파일 + 새 행 → (ts) 중복 제거 후 통째로 다시 쓴다. 새 행이 이긴다."""
    p = HISTORY / f"base={base}" / f"part-{ym}.parquet"
    merged: dict[int, dict] = {}
    if p.exists():
        for r in pq.read_table(p).to_pylist():
            merged[r["ts"]] = r
    merged.update({r["ts"]: r for r in new_rows})
    rows = [merged[k] for k in sorted(merged)]
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    pq.write_table(pa.table({f.name: [r[f.name] for r in rows] for f in SCHEMA}, schema=SCHEMA),
                   tmp, compression="zstd")
    tmp.replace(p)
    return len(rows)


def compact_once() -> dict:
    """지난 날짜 폴더를 전부 접는다. 반환 {days, rows, bases}."""
    if backfill_running():
        log.warning("백필이 도는 중 — compaction 건너뜀(같은 월 파일 충돌)")
        return {"days": 0, "rows": 0, "bases": 0, "skipped": "backfill"}

    days = _closed_date_dirs()
    if not days:
        return {"days": 0, "rows": 0, "bases": 0}

    # 월 단위로 다시 쓰므로, 같은 달에 걸친 날짜들을 모아 한 번에 처리한다.
    buckets: dict[tuple[str, str], dict[int, dict]] = {}
    for d in days:
        ym = d.name.split("=", 1)[1][:7]
        for f in d.glob("*.parquet"):
            for r in pq.read_table(f).to_pylist():
                buckets.setdefault((r["base"], ym), {})[r["ts"]] = r

    rows = 0
    for (base, ym), by_ts in buckets.items():
        try:
            rows += len(by_ts)
            _merge_month(base, ym, list(by_ts.values()))
        except Exception as e:
            log.error("%s %s 병합 실패 — 원본 보존: %s", base, ym, e)
            return {"days": 0, "rows": rows, "bases": len(buckets), "error": str(e)[:200]}

    for d in days:                     # 병합이 전부 끝난 뒤에만 버퍼를 버린다
        shutil.rmtree(d)
    bases = len({b for b, _ in buckets})
    log.info("compaction: %d일 · %d종목 · %d행을 history 로 접고 버퍼 삭제", len(days), bases, rows)
    return {"days": len(days), "rows": rows, "bases": bases}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(compact_once())
