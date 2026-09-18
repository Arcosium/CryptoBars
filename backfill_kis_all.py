#!/usr/bin/env python3
"""전종목 백필·갱신 — backfill_kis.py 를 그대로 쓰되 유니버스만 바꾼다.

universe.csv 는 네이버 수집기(equities.py)도 읽는다. 거기에 2,565종목을 넣으면
5분 주기 크롤링이 초당 8.5요청으로 뛰어 차단 위험이 있으므로 목록을 분리했다.

실행 전에 KIS 종목 마스터(무인증)로 universe_all.csv 를 보강한다 — 신규 상장을 놓치지 않기 위해
(2026-09-18: 미국 갱신에서 '상위 1499'로 드러난 것과 같은 구멍). 지우지는 않는다 —
상폐 종목의 과거도 필요하고 KIS 는 상폐 코드도 준다. 마스터를 못 받으면 기존 목록으로 진행한다.
일일 타이머(cryptobars-krx)도 이 파일을 돌린다: done 장부 덕에 새 거래일과 실패분만 받는다.
"""
import asyncio, csv, io, re, zipfile, urllib.request
import backfill_kis as bk

UNIVERSE = '/home/arcosium/vault/CryptoBars/data/KRX/universe_all.csv'
MASTER = 'https://new.real.download.dws.co.kr/common/master/{}_code.mst.zip'
# 우선주(끝자리≠0)·ETF/ETN/리츠/스팩류 제외 — 처음 2,545종목을 고른 기준과 같다
ETF = re.compile(r'KODEX|TIGER|ARIRANG|KBSTAR|HANARO|KOSEF|SOL |ACE |PLUS |RISE |TIMEFOLIO|KIWOOM|'
                 r'ETN|레버리지|인버스|선물|채권|리츠|스팩|액티브|미국|나스닥|S&P|배당')


def refresh_universe():
    with open(UNIVERSE, newline='') as f:
        have = {r['code'] for r in csv.DictReader(f)}
    new = []
    for m in ('kospi', 'kosdaq'):
        try:
            req = urllib.request.Request(MASTER.format(m), headers={'User-Agent': 'Mozilla/5.0'})
            z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(req, timeout=60).read()))
            text = z.read(f'{m}_code.mst').decode('cp949', errors='ignore')
        except Exception as e:
            print(f'마스터 {m} 실패 {type(e).__name__} — 기존 목록으로 진행', flush=True)
            continue
        for line in text.splitlines():
            code = line[:9].strip()
            if len(code) == 6 and code.isdigit() and code.endswith('0') and code not in have:
                name = line[21:].split('  ')[0].strip()
                if not ETF.search(name):
                    new.append((code, name, m.upper()))
                    have.add(code)
    if new:
        with open(UNIVERSE, 'a', newline='') as f:
            w = csv.writer(f)
            for c, n, mk in new:
                w.writerow([c, n, mk, ''])
        print(f'신규 상장 편입 {len(new)}: {[c for c, _, _ in new[:10]]}', flush=True)
    return new


if __name__ == '__main__':
    refresh_universe()
    bk.UNIVERSE = UNIVERSE
    asyncio.run(bk.main())
