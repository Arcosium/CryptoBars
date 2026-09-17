#!/usr/bin/env python3
"""Polygon 미국 1분봉 백필 + 일일 갱신.

월 단위로 받아 USA/1m/{TICKER}/{YYYY-MM}.parquet 에 저장한다. 파케이는 append 가 안 되므로
월 파일로 쪼갠다 — 그래서 재시작은 "파일 있으면 건너뜀"으로 끝나고 done 장부가 필요 없다.
최근 --refresh 개월은 파일이 있어도 다시 받는다(그 달은 아직 자라는 중이므로).

같은 스크립트가 백필과 일일 갱신을 겸한다. 갱신은 --refresh 1 --years 0.

사용:
  python3 backfill_polygon.py                # 5년 전체 백필
  python3 backfill_polygon.py --years 0      # 이번 달만 갱신(타이머용)
"""
import os, re, sys, time, argparse, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, pandas as pd

ENV = '/home/arcosium/vault/CryptoBars/.env'
DATA = '/home/arcosium/vault/CryptoBars/data/USA'
OUT = os.path.join(DATA, '1m')
UNIVERSE = os.path.join(DATA, 'universe.txt')
BASE = 'https://api.polygon.io'
COLS = ['t', 'o', 'h', 'l', 'c', 'v', 'vw', 'n']


def api_key():
    return re.search(r'POLYGON_API_KEY=(\S+)', open(ENV).read()).group(1)


def months(years):
    """오늘부터 거슬러 years 년치 월 목록 (오래된 순)."""
    today = dt.date.today()
    out, y, m = [], today.year, today.month
    for _ in range(years * 12 + 1):
        out.append(f'{y:04d}-{m:02d}')
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return sorted(out)


def month_range(ym):
    first = dt.date(int(ym[:4]), int(ym[5:]), 1)
    nxt = dt.date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return first.isoformat(), (nxt - dt.timedelta(days=1)).isoformat()


def fetch(sess, key, ticker, ym, tries=4):
    a, b = month_range(ym)
    url = f'{BASE}/v2/aggs/ticker/{ticker}/range/1/minute/{a}/{b}'
    for i in range(tries):
        r = sess.get(url, params={'limit': 50000, 'adjusted': 'true', 'apiKey': key}, timeout=90)
        if r.status_code == 429:
            time.sleep(2 * (i + 1))
            continue
        if r.status_code == 403:
            return None  # 요금제 소급 한계 밖 — 조용히 끝낸다
        r.raise_for_status()
        j = r.json()
        rows = j.get('results') or []
        if j.get('next_url'):  # 5만 건을 넘으면 이어받는다 (한 달 1분봉은 보통 2만 건)
            nxt = j['next_url']
            while nxt:
                rr = sess.get(nxt, params={'apiKey': key}, timeout=90).json()
                rows += rr.get('results') or []
                nxt = rr.get('next_url')
        return rows
    return []


def one(key, ticker, ms, refresh):
    sess = requests.Session()
    d = os.path.join(OUT, ticker)
    os.makedirs(d, exist_ok=True)
    fresh = set(ms[-refresh:]) if refresh else set()
    # 지난달보다 과거인 달이 비어 있으면 영원히 빈 달이다(상장 전·상폐 후). `.empty` 마커로 기억해
    # 다시 묻지 않는다. 이게 없으면 재시작·갱신 때마다 빈 달을 전부 재요청한다 — 받은 5,139종목에서
    # 11만 건(81분)이었고, 무료 5회/분에서는 상폐 종목 하나가 13요청씩 먹어 일일 갱신이 안 끝난다.
    settled = ms[-2] if len(ms) >= 2 else ''
    wrote = bars = 0
    for ym in ms:
        p = os.path.join(d, f'{ym}.parquet')
        e = os.path.join(d, f'{ym}.empty')
        if ym not in fresh and (os.path.exists(p) or os.path.exists(e)):
            continue
        rows = fetch(sess, key, ticker, ym)
        if rows is None:
            continue  # 요금제 소급 한계 밖(403). ms 가 오래된 순이라 더 최근 달은 받을 수 있다 — 마커는 남기지 않는다
        if not rows:
            if ym < settled:
                open(e, 'w').close()
            continue
        df = pd.DataFrame(rows).reindex(columns=COLS)
        df.to_parquet(p, index=False, compression='zstd')
        wrote += 1
        bars += len(df)
    return ticker, wrote, bars


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--years', type=int, default=5)
    ap.add_argument('--refresh', type=int, default=1, help='최근 N개월은 파일 있어도 재요청')
    ap.add_argument('--workers', type=int, default=6)
    ap.add_argument('--tickers', type=int, default=0, help='앞에서 N종목만 (0=전체)')
    a = ap.parse_args()

    key = api_key()
    names = [x.strip() for x in open(UNIVERSE) if x.strip()]
    if a.tickers:
        names = names[:a.tickers]
    ms = months(a.years)
    print(f'{len(names)}종목 × {len(ms)}개월 ({ms[0]}~{ms[-1]}) workers={a.workers}', flush=True)

    t0 = time.time()
    done = tot = 0
    with ThreadPoolExecutor(a.workers) as ex:
        futs = {ex.submit(one, key, t, ms, a.refresh): t for t in names}
        for f in as_completed(futs):
            try:
                tk, wrote, bars = f.result()
            except Exception as exc:
                print(f'  {futs[f]} 실패 {type(exc).__name__} {str(exc)[:70]}', flush=True)
                continue
            done += 1
            tot += bars
            if wrote:
                el = time.time() - t0
                print(f'[{done}/{len(names)}] {tk:6} 새 월파일 {wrote:3}  {bars:>9,}봉  '
                      f'누적 {tot:>12,}  {el/60:.1f}분  남은예상 {el/done*(len(names)-done)/60:.0f}분',
                      flush=True)
    print(f'완료: {done}종목  {tot:,}봉  {(time.time()-t0)/60:.1f}분', flush=True)


if __name__ == '__main__':
    main()
