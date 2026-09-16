#!/usr/bin/env python3
"""KIS 1분봉 백필 (OHLCV).

KIS 일별분봉 TR 은 약 250거래일 롤링이라 매일 가장 오래된 1거래일이 영구히 사라진다
(2026-09-16 에 나오던 2025-09-05 가 09-17 에 0행). 그래서 오래된 날짜부터 받는다.

거래일은 로컬 일봉 파케이 + 네이버 분봉 DB 의 합집합에서 뽑아 휴장일 요청을 낭비하지 않는다.
재시작 안전: (code, day) 를 done 에 기록하고 건너뛴다.

사용:  python3 backfill_kis.py [--codes N] [--days N]
"""
import sys, asyncio, sqlite3, csv, glob, argparse, datetime as dt, zoneinfo

sys.path.insert(0, '/home/arcosium/projects/QuantInSight')
sys.path.insert(0, '/home/arcosium/projects/CryptoBars')
from equities import read_broker

KST = zoneinfo.ZoneInfo('Asia/Seoul')
OUT = '/home/arcosium/vault/CryptoBars/data/KRX/bars_ohlc.db'
DAILY = '/home/arcosium/projects/HYFE_QTPA/work/bars_krx/1d/005930.parquet'
MINUTE = '/home/arcosium/vault/CryptoBars/data/KRX/bars.db'
UNIVERSE = '/home/arcosium/vault/CryptoBars/data/KRX/universe.csv'
PATH = '/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice'
TR = 'FHKST03010230'
HOURS = ('093000', '113000', '133000', '153000')  # 각 호출이 요청시각 포함 직전 120분


def trading_days(start):
    """start(YYYYMMDD) 이상의 거래일. 달력은 종목과 무관하므로 005930 하나면 충분."""
    import pandas as pd
    days = {dt.datetime.fromtimestamp(t / 1000, KST).strftime('%Y%m%d')
            for t in pd.read_parquet(DAILY)['ts']}
    with sqlite3.connect(f'file:{MINUTE}?mode=ro', uri=True) as c:
        days |= {r[0] for r in c.execute('SELECT DISTINCT substr(ts,1,8) FROM bars')}
    return sorted(d for d in days if d >= start)


def open_out():
    c = sqlite3.connect(OUT)
    c.execute('''CREATE TABLE IF NOT EXISTS bars(
        code TEXT NOT NULL, ts TEXT NOT NULL,
        o REAL, h REAL, l REAL, c REAL, v REAL,
        PRIMARY KEY(code, ts))''')
    c.execute('CREATE TABLE IF NOT EXISTS done(code TEXT, day TEXT, PRIMARY KEY(code, day))')
    return c


async def fetch_day(broker, code, day):
    rows = {}
    for h in HOURS:
        j = await broker._get_json(PATH, TR, {
            'FID_COND_MRKT_DIV_CODE': 'J', 'FID_INPUT_ISCD': code,
            'FID_INPUT_DATE_1': day, 'FID_INPUT_HOUR_1': h,
            'FID_PW_DATA_INCU_YN': 'Y', 'FID_FAKE_TICK_INCU_YN': 'N'})
        for r in (j.get('output2') or []):
            ts = r['stck_bsop_date'] + r['stck_cntg_hour']
            rows[ts] = (code, ts, float(r['stck_oprc']), float(r['stck_hgpr']),
                        float(r['stck_lwpr']), float(r['stck_prpr']), float(r['cntg_vol']))
    return list(rows.values())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--codes', type=int, default=0, help='앞에서 N종목만 (0=전체)')
    ap.add_argument('--days', type=int, default=0, help='오래된 N일만 (0=전체)')
    ap.add_argument('--start', default='20250908', help='이 날짜부터')
    a = ap.parse_args()

    with open(UNIVERSE, newline='') as f:
        codes = [r['code'] for r in csv.DictReader(f)]
    days = trading_days(a.start)
    if a.codes: codes = codes[:a.codes]
    if a.days: days = days[:a.days]

    broker, conn = read_broker(), open_out()
    print(f'종목 {len(codes)} × 거래일 {len(days)} ({days[0]}~{days[-1]}) '
          f'= 최대 {len(codes)*len(days)*len(HOURS):,} 요청', flush=True)

    for day in days:  # 오래된 날부터 — 먼저 사라질 것부터
        got = skipped = failed = 0
        for code in codes:
            if conn.execute('SELECT 1 FROM done WHERE code=? AND day=?', (code, day)).fetchone():
                skipped += 1
                continue
            try:
                rows = await fetch_day(broker, code, day)
            except Exception as exc:
                failed += 1
                print(f'  {day} {code} 실패 {type(exc).__name__}', flush=True)
                continue
            conn.executemany('INSERT OR IGNORE INTO bars VALUES(?,?,?,?,?,?,?)', rows)
            conn.execute('INSERT OR IGNORE INTO done VALUES(?,?)', (code, day))
            conn.commit()
            got += len(rows)
        total = conn.execute('SELECT COUNT(*) FROM bars').fetchone()[0]
        print(f'{day}  신규봉 {got:>7,}  건너뜀 {skipped:>4}  실패 {failed:>3}  '
              f'누적 {total:,}  {dt.datetime.now(KST):%H:%M:%S}', flush=True)


if __name__ == '__main__':
    asyncio.run(main())
