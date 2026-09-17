#!/usr/bin/env python3
"""Polygon 미국 1분봉 갱신 — 무료(Basic) 티어용 라운드로빈.

무료는 5회/분이라 하루에 약 7,200요청이 상한이다. 유니버스가 2만 종목이면 전체를 매일 갱신할 수
없다(2.8일). 다만 **Polygon 은 KIS 와 달리 롤링이 아니다** — 2년 안의 구간은 언제든 다시 받을 수
있어 갱신이 며칠 늦어도 잃는 게 없다. 그래서 이렇게 나눈다:

  - 거래대금 상위 --top 종목: 매일 갱신 (실제로 쓰는 종목)
  - 나머지: --rotate 개씩 이어받아 며칠에 한 바퀴 (커서는 상태 파일에 저장)

Starter 구독 동안에는 backfill_polygon.py 를 그대로 쓰면 되고(전체가 7분),
무료 전환 뒤 타이머의 ExecStart 를 이 스크립트로 바꾼다.

사용:  python3 refresh_polygon.py --top 1500 --rotate 3500
"""
import os, json, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

import backfill_polygon as bp   # 유니버스·월 계산·수집은 전부 재사용

STATE = os.path.join(bp.DATA, 'refresh_state.json')


def top_by_dollar_volume(key, n):
    """최신 거래일의 grouped daily 1요청으로 거래대금 상위 n 종목을 뽑는다."""
    s = requests.Session()
    for back in range(1, 8):                      # 휴장일이면 하루씩 거슬러 간다
        d = time.strftime('%Y-%m-%d', time.localtime(time.time() - back * 86400))
        j = s.get(f'{bp.BASE}/v2/aggs/grouped/locale/us/market/stocks/{d}',
                  params={'adjusted': 'true', 'apiKey': key}, timeout=60).json()
        rows = [r for r in (j.get('results') or []) if r.get('v') and r.get('vw')]
        if len(rows) > 3000:
            rows.sort(key=lambda r: r['v'] * r['vw'], reverse=True)
            return [r['T'] for r in rows[:n]], d
    return [], None


def load_cursor():
    try:
        return json.load(open(STATE)).get('cursor', 0)
    except (OSError, ValueError):
        return 0


def save_cursor(c, **kw):
    tmp = STATE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({'cursor': c, 'ts': int(time.time()), **kw}, f)
    os.replace(tmp, STATE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--top', type=int, default=1500, help='매일 갱신할 거래대금 상위 종목 수')
    ap.add_argument('--rotate', type=int, default=3500, help='나머지 중 이번에 처리할 개수')
    ap.add_argument('--refresh', type=int, default=1, help='최근 N개월 재요청')
    ap.add_argument('--workers', type=int, default=1, help='무료 5회/분이라 1이 맞다')
    ap.add_argument('--min-interval', type=float, default=12.0,
                    help='요청 간 최소 간격(초). 12 = 5회/분 = 무료 상한')
    a = ap.parse_args()

    # 무료 상한에 맞춰 요청 간격을 강제한다. 429 를 맞고 재시도하는 것보다 예측 가능하고
    # 낭비가 없다. bp.fetch 를 감싸는 이유는 bp.one 이 종목당 여러 달을 요청하기 때문 —
    # 종목 단위로 재면 월 요청이 그대로 몰린다. workers 1 이라 락은 필요 없다.
    if a.min_interval > 0:
        _orig, _last = bp.fetch, [0.0]

        def paced(*args, **kw):
            gap = a.min_interval - (time.time() - _last[0])
            if gap > 0:
                time.sleep(gap)
            _last[0] = time.time()
            return _orig(*args, **kw)
        bp.fetch = paced

    key = bp.api_key()
    names = [x.strip() for x in open(bp.UNIVERSE) if x.strip()]
    universe = set(names)

    top, day = top_by_dollar_volume(key, a.top)
    top = [t for t in top if t in universe]       # 유니버스 밖 신규 상장은 백필 몫
    rest = [t for t in names if t not in set(top)]

    cur = load_cursor() % max(len(rest), 1)
    batch = rest[cur:cur + a.rotate]
    if len(batch) < a.rotate:                     # 끝에 닿으면 앞으로 감는다
        batch += rest[:a.rotate - len(batch)]
    nxt = (cur + a.rotate) % max(len(rest), 1)

    # 월이 바뀌고 며칠간은 직전 달도 함께 본다 — 그 달 마지막 거래일이 빠지지 않게.
    refresh = 2 if int(time.strftime('%d')) <= 3 else a.refresh
    ms = bp.months(1)
    targets = top + batch
    print(f'기준일 {day}  상위 {len(top)} + 순환 {len(batch)} = {len(targets)}종목  '
          f'(순환 대상 {len(rest)}, 커서 {cur}→{nxt}, 약 {len(rest)/max(a.rotate,1):.1f}일에 한 바퀴)',
          flush=True)

    t0 = time.time()
    done = bars = fail = 0
    with ThreadPoolExecutor(a.workers) as ex:
        futs = {ex.submit(bp.one, key, t, ms, refresh): t for t in targets}
        for f in as_completed(futs):
            try:
                _, _, b = f.result()
                bars += b
            except Exception as exc:
                fail += 1
                print(f'  {futs[f]} 실패 {type(exc).__name__}', flush=True)
            done += 1
            if done % 500 == 0:
                print(f'  {done}/{len(targets)}  {bars:,}봉  {(time.time()-t0)/60:.0f}분', flush=True)

    save_cursor(nxt, top=len(top), rotate=len(batch), bars=bars, fail=fail,
                took_s=int(time.time() - t0))
    print(f'완료: {done}종목  {bars:,}봉  실패 {fail}  {(time.time()-t0)/60:.1f}분', flush=True)


if __name__ == '__main__':
    main()
